# dags/cars_lots.py
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.cars_analytics.common.telegram_alerts import build_failure_message, send_telegram_message, \
    build_success_message
from scripts.cars_analytics.parsers.auctions import ParserAuctions

local_tz = pendulum.timezone("Asia/Novosibirsk")


def _run_parsing_auction():
    brand_models = Variable.get("parser.auction.brand_model", deserialize_json=True)
    option_cars_list = Variable.get("parser.auction.filters.options", deserialize_json=True)
    batch_size = int(Variable.get("parser.batch_size", default_var=20))
    min_year = int(Variable.get("parser.min_year", default_var=2014))

    hook = PostgresHook(postgres_conn_id="japan_cars_db")
    engine = hook.get_sqlalchemy_engine()

    parser = ParserAuctions(
        airflow_mode=engine,
        brand_models=brand_models,
        option_cars_list=option_cars_list,
        batch_size=batch_size,
        min_year=min_year
    )
    result = parser.parse_auctions_and_save()
    return result


def _on_failure_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception") or Exception("Неизвестная ошибка")

    message = build_failure_message(dag_id, task_id, execution_date, exception)
    send_telegram_message(message)


def _on_success_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]

    # Получаем return_value из XCom
    result = context["task_instance"].xcom_pull(task_ids=task_id, key="return_value")

    if not result or not isinstance(result, dict):
        extra = "Данные о результатах отсутствуют."
    else:
        total = result.get("total_lots", 0)
        details = result.get("details", [])
        detail_lines = []
        for item in details:
            brand = item.get("brand_model", "N/A")
            count = item.get("lots_count", 0)
            detail_lines.append(f"• {brand}: {count} лотов")
        details_text = "\n".join(detail_lines) if detail_lines else "Нет деталей"
        extra = f"Всего обработано лотов: {total}\nПо брендам:\n{details_text}"

    start = context["dag_run"].start_date
    end = context["task_instance"].end_date or context["task_instance"].start_date
    duration = (end - start).total_seconds() if start else 0

    message = build_success_message(dag_id, task_id, execution_date, duration, extra)
    send_telegram_message(message)


with DAG(
        'japan_cars_auction',
        start_date=datetime(2026, 1, 1, tzinfo=local_tz),
        schedule_interval='0,16,19 * * * *',
        catchup=False,
        tags=['japan', 'cars', 'auctions'],
) as dag:
    parse_and_load_auction = PythonOperator(
        task_id='parse_and_load_auction',
        python_callable=_run_parsing_auction,
        on_failure_callback=_on_failure_callback,
        on_success_callback=_on_success_callback,
    )
    # . --exclude test_type:generic отключены тесты
    # run_dbt_models = BashOperator(
    #     task_id='run_dbt_models',
    #     bash_command='''
    #         cd /home/ubuntu/airflow/airflow_home/dbt/dbt_cars_analytics &&
    #         unset PYTHONPATH &&
    #         set -a && source /etc/myapp/.env && set +a &&
    #         /home/ubuntu/projects/airflow/env/bin/dbt build --profiles-dir . --project-dir . --exclude test_type:generic
    #     ''',
    #     on_failure_callback=_on_failure_callback,
    # )

    # Порядок выполнения
    # (
    #         parse_and_load_auction
    #         #>> run_dbt_models
    # )
