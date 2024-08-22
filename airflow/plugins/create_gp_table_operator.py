from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class CreateGPTableOperator(BaseOperator):
    def __init__(self, table_name: str, schema: dict, replace: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        self.table_name = table_name
        self.schema = schema
        self.replace = replace

    def execute(self, context) -> None:
        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                if self.replace:
                    cur.execute(f'drop table if exists {self.table_name}')
                cur.execute(
                    f'create table {self.table_name} '
                    f"({', '.join([' '.join(_) for _ in self.schema.items()])}) "
                    'distributed randomly'
                )
