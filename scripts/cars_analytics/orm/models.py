from sqlalchemy import Column, BigInteger, Integer, Numeric, SmallInteger, String, DateTime, Date, func, Index
from sqlalchemy.schema import ForeignKey
from scripts.cars_analytics.orm.database import Base


class BrandRaw(Base):
    __tablename__ = "brands_raw"
    __table_args__ = {"schema": "raw"}

    id_brand = Column(Integer, primary_key=True, index=True)
    brand = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)


class ModelRaw(Base):
    __tablename__ = "models_raw"
    __table_args__ = (
        {"schema": "raw"}
    )

    id_model = Column(Integer, primary_key=True, index=True)
    id_brand = Column(Integer, ForeignKey("raw.brands_raw.id_brand", ondelete="CASCADE"), nullable=False)
    model = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)


class CarbodyRaw(Base):
    __tablename__ = "carbodies_raw"
    __table_args__ = {"schema": "raw"}

    id_carbody = Column(Integer, primary_key=True, index=True)
    id_model = Column(Integer, ForeignKey("raw.models_raw.id_model", ondelete="CASCADE"), nullable=False)
    carbody = Column(String, nullable=False)
    description = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)


class CarRaw(Base):
    __tablename__ = "cars_raw"
    __table_args__ = (
        Index("idx_f_cars_opt", "id_brand", "id_model", "id_carbody", "year_release", "rate", "created_at",
              postgresql_include=["mileage", "cost"]),
        {"schema": "raw"}
    )

    id = Column(Integer, primary_key=True, index=True)
    id_car = Column(BigInteger, unique=True, nullable=False)
    id_brand = Column(Integer, ForeignKey("raw.brands_raw.id_brand"), nullable=False)
    id_model = Column(Integer, ForeignKey("raw.models_raw.id_model"), nullable=False)
    id_carbody = Column(Integer, ForeignKey("raw.carbodies_raw.id_carbody"), nullable=False)
    cost = Column(Numeric, nullable=False)
    year_release = Column(SmallInteger, nullable=False)
    rate = Column(String, nullable=False)
    mileage = Column(Integer, nullable=False)
    transmission = Column(String, nullable=True)
    drive_type = Column(String, nullable=True)
    fuel_type = Column(String, nullable=True)
    source_lot_id = Column(String, nullable=False)
    link_source = Column(String, nullable=False)
    lot_date = Column(Date, nullable=False)
    equipment = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


class AuctionCarRaw(Base):
    __tablename__ = "auction_cars_raw"
    __table_args__ = (
        Index("idx_auction_cars_raw_auction_date", "auction_date"),
        Index("idx_auction_cars_raw_model_id_car", "id_model", "year_release", "id_car"),
        {"schema": "raw"}
    )

    id = Column(Integer, primary_key=True, index=True)
    id_car = Column(BigInteger, unique=True, nullable=False)
    id_brand = Column(Integer, ForeignKey("raw.brands_raw.id_brand"), nullable=False)
    id_model = Column(Integer, ForeignKey("raw.models_raw.id_model"), nullable=False)
    id_carbody = Column(Integer, ForeignKey("raw.carbodies_raw.id_carbody"), nullable=False)
    year_release = Column(SmallInteger, nullable=False)
    mileage = Column(Integer, nullable=False)
    transmission = Column(String, nullable=True)
    drive_type = Column(String, nullable=True)
    fuel_type = Column(String, nullable=True)
    start_price = Column(Numeric, nullable=True)
    final_price = Column(Numeric, nullable=True)
    source_lot_id = Column(String, nullable=False)
    link_source = Column(String, nullable=True)
    auction_date = Column(Date, nullable=False)
    rate = Column(String, nullable=True)
    equipment = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), nullable=False)
