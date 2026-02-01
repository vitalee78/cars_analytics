# orm/database.py
import os
from contextlib import contextmanager
from typing import Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

Base = declarative_base()

DB_NAME_AIRFLOW = "japan_cars_db"

def get_engine(airflow_mode: bool = False, conn_id: str = DB_NAME_AIRFLOW) -> Engine:
    """
    Создаёт engine для подключения к БД.

    Args:
        airflow_mode: True — использовать Airflow Connection, False — .env
        conn_id: ID подключения в Airflow (только для airflow_mode=True)

    Returns:
        SQLAlchemy Engine
    """
    if airflow_mode:
        # Отложенный импорт — запуск без Airflow
        try:
            from airflow.hooks.base import BaseHook
            conn = BaseHook.get_connection(conn_id)
            database_url = (
                f"postgresql://{conn.login}:{conn.password}"
                f"@{conn.host}:{conn.port}/{conn.schema}"
            )
        except ImportError:
            raise RuntimeError(
                "Airflow не установлен. Установите 'apache-airflow-providers-postgres' "
                "или запускайте без airflow_mode=True"
            )
        except Exception as e:
            raise RuntimeError(f"Ошибка получения Airflow Connection '{conn_id}': {e}")
    else:
        # Локальный режим через .env
        required_vars = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(
                f"Отсутствуют переменные окружения: {missing}. "
                "Проверьте .env файл в корне проекта."
            )

        database_url = (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )

    return create_engine(database_url, pool_pre_ping=True)


@contextmanager
def get_session(airflow_mode: bool = False, conn_id: str = DB_NAME_AIRFLOW):
    """
    Context manager для работы с сессией БД.

    Пример использования в DAG:
        with get_session(airflow_mode=True) as session:
            session.query(CarRaw).all()
    """
    engine = get_engine(airflow_mode=airflow_mode, conn_id=conn_id)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
