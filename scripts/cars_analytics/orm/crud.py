# orm/crud.py
from typing import List, Dict, Tuple
import pandas as pd
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from scripts.cars_analytics.orm.database import get_session
from scripts.cars_analytics.orm.models import BrandRaw
from scripts.cars_analytics.orm.models import ModelRaw
from scripts.cars_analytics.orm.models import CarbodyRaw
from scripts.cars_analytics.orm.models import CarRaw


def _bulk_upsert_brands(brand_names: List[str]) -> Dict[str, int]:
    """Возвращает {brand: id_brand}"""
    if not brand_names:
        return {}
    unique_brands = list(set(brand_names))
    stmt = pg_insert(BrandRaw).values([{"brand": name} for name in unique_brands])
    stmt = stmt.on_conflict_do_nothing(index_elements=["brand"])
    with get_session() as session:
        session.execute(stmt)
        session.commit()

    with get_session() as session:
        result = session.execute(
            select(BrandRaw.brand, BrandRaw.id_brand).where(BrandRaw.brand.in_(unique_brands))
        )
        return {row.brand: row.id_brand for row in result}


def _bulk_upsert_models(brand_model_pairs: List[Tuple[int, str]]) -> Dict[Tuple[int, str], int]:
    """Возвращает {(id_brand, model): id_model}"""
    if not brand_model_pairs:
        return {}
    unique_pairs = list(set(brand_model_pairs))
    records = [{"id_brand": b, "model": m} for b, m in unique_pairs]
    stmt = pg_insert(ModelRaw).values(records)
    stmt = stmt.on_conflict_do_nothing(index_elements=["id_brand", "model"])
    with get_session() as session:
        session.execute(stmt)
        session.commit()

    with get_session() as session:
        result = session.execute(
            select(ModelRaw.id_brand, ModelRaw.model, ModelRaw.id_model)
        )
        # Загружаем весь справочник — он небольшой
        mapping = {}
        for row in result:
            mapping[(row.id_brand, row.model)] = row.id_model
        # Фильтруем только нужные пары
        return {pair: mapping[pair] for pair in unique_pairs if pair in mapping}


def _bulk_upsert_carbodies(model_carbody_pairs: List[Tuple[int, str]]) -> Dict[Tuple[int, str], int]:
    """Возвращает {(id_model, carbody): id_carbody}"""
    if not model_carbody_pairs:
        return {}
    unique_pairs = list(set(model_carbody_pairs))
    records = [{"id_model": m, "carbody": c} for m, c in unique_pairs if c]  # пропускаем пустые кузова
    if records:
        stmt = pg_insert(CarbodyRaw).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["id_model", "carbody"])
        with get_session() as session:
            session.execute(stmt)
            session.commit()

    with get_session() as session:
        result = session.execute(
            select(CarbodyRaw.id_model, CarbodyRaw.carbody, CarbodyRaw.id_carbody)
        )
        mapping = {}
        for row in result:
            mapping[(row.id_model, row.carbody)] = row.id_carbody
        return {pair: mapping.get(pair, None) for pair in unique_pairs}


def bulk_upsert_cars(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    df = df.copy()
    df["brand"] = df["brand"].astype(str)
    df["model"] = df["model"].astype(str)
    df["carbody"] = df["carbody"].fillna("").astype(str)

    # 1. Upsert брендов
    brand_to_id = _bulk_upsert_brands(df["brand"].unique().tolist())

    # 2. Upsert моделей
    brand_model_pairs = [
        (brand_to_id[row["brand"]], row["model"]) for _, row in df.iterrows()
    ]
    model_to_id = _bulk_upsert_models(brand_model_pairs)

    # 3. Upsert кузовов
    model_carbody_pairs = [
        (model_to_id[(brand_to_id[row["brand"]], row["model"])], row["carbody"])
        for _, row in df.iterrows()
    ]
    carbody_to_id = _bulk_upsert_carbodies(model_carbody_pairs)

    # 4. Формируем записи для вставки
    records: List[dict] = []
    for _, row in df.iterrows():
        brand_id = brand_to_id[row["brand"]]
        model_id = model_to_id[(brand_id, row["model"])]
        carbody_id = carbody_to_id.get((model_id, row["carbody"])) or 0  # fallback если кузов пустой

        records.append({
            "id_car": int(row["id_car"]),
            "id_brand": brand_id,
            "id_model": model_id,
            "id_carbody": carbody_id,
            "cost": row.get("cost"),
            "year_release": int(row["year"]),
            "rate": row.get("rate"),
            "mileage": int(row.get("mileage", 0)),
            "transmission": row.get("transmission"),
            "drive_type": row.get("drive_type"),
            "fuel_type": row.get("fuel_type"),
            "source_lot_id": str(row["source_lot_id"]),
            "link_source": str(row["link_source"]),
            "lot_date": row["lot_date"],
            "equipment": row.get("equipment"),
        })

    # 5. Upsert в основную таблицу
    stmt = pg_insert(CarRaw).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id_car"],
        set_={
            "mileage": stmt.excluded.mileage,
            "link_source": stmt.excluded.link_source,
            "equipment": stmt.excluded.equipment,
            "rate": stmt.excluded.rate,
            "updated_at": func.now(),
        }
    )

    with get_session() as session:
        result = session.execute(stmt)
        session.commit()
        return result.rowcount