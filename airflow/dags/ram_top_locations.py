from pendulum import datetime, duration, DateTime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from dm_kulikov.top_ram_locations_operator import GetTopLocationsOperator
from dm_kulikov.insert_to_gp_operator import InsertToGPOperator
from dm_kulikov.create_gp_table_operator import CreateGPTableOperator

DEFAULT_ARGS = {
    'start_date': datetime(2024, 3, 1),
    'owner': 'dm-kulikov',
    'retry_delay': duration(minutes=10),
    'execution_timeout': duration(minutes=10),
}

with DAG(
    dag_id='ram_top_locations',
    schedule_interval='@once',
    description='Получаем данные из api, записываем в таблицу в green plum',
    default_args=DEFAULT_ARGS,
    catchup=False,
    render_template_as_native_obj=True
) as dag:
    schema = {
        'id': 'int',
        'name': 'varchar',
        'type': 'varchar',
        'dimension': 'varchar',
        'resident_cnt': 'int',
    }

    table_name = 'ram_location'

    def prepare_locations_for_insert(ti: TaskInstance = None):
        locations = ti.xcom_pull(task_ids='get_top_locations')
        return [
            {
                'id': loc['id'],
                'name': loc['name'],
                'type': loc['type'],
                'dimension': loc['dimension'],
                'resident_cnt': len(loc['residents']),
            }
            for loc in locations
        ]

    (
        GetTopLocationsOperator(
            task_id='get_top_locations',
        )
        >> PythonOperator(
            task_id='prepare_locations_for_insert',
            python_callable=prepare_locations_for_insert,
        )
        >> CreateGPTableOperator(
            task_id='create_table',
            table_name=table_name,
            schema=schema,
            replace=True,
        )
        >> InsertToGPOperator(
            task_id='insert',
            table_name=table_name,
            columns=schema.keys(),
            values="{{task_instance.xcom_pull(task_ids='prepare_locations_for_insert')}}",
        )
    )
