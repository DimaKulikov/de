from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List


class InsertToGPOperator(BaseOperator):
    template_fields = ['values']
    template_fields_renderers = {
        'values':'json'
    }

    def __init__(self, table_name: str, columns: List[str], values: List[dict], **kwargs):
        super().__init__(**kwargs)
        self.pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def execute(self, context) -> None:
        sql_statement = (
            f'insert into {self.table_name}({",".join(self.columns)}) '
            f'values ({",".join([f"%({c})s"for c in self.columns])})'
        )
        print(self.values)
        print(self.table_name)
        print(self.columns)
        print(sql_statement)
        print('values type',type(self.values))

        with self.pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    sql_statement,
                    self.values,
                )
