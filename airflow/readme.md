Примеры использования airflow
# Таск группа для рассылки на почту
Таск группа, которую можно использовать для создания шаблонных дагов для рассылки файлов на почты. 

Логика рассылки отталкивается от сущности "МО", для текущей демонстрации не принципиально, что это означает, по сути МО - получатель рассылки. 

Используется так: в даге объявляем сенсоры (если нужны) и таск с конфигом, как описано в докстринге [здесь](./task_groups/send_mails.py), примерно так:
```python
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.airflow_utils import local_tz
from constants.airflow_constants import MO_AVAIL_MAILING
from pendulum import datetime, duration, DateTime
from task_groups.send_by_mo import send_mails_by_mo

MO_LIST = MO_AVAIL_MAILING
with DAG(
    'daily_send_ref_activ',
    description='Отправка активных направлений по МО',
    default_args={
        'owner': 'gennady',
        'start_date': datetime(2023, 7, 31, tz=local_tz),
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': duration(minutes=10),
        'pool': 'daily_send_ref_activ',
    },
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=["daily", "referrals", "mail", "smtp"],
    max_active_tasks=1,
) as dag:

    def mailing_config(
        data_interval_end: DateTime = None,
    ):
        import os
        from removed_for_confidentiality import Email
        from removed_for_confidentiality import AUTO_REPORTS_ROOT
        from removed_for_confidentiality import (
            SUBJECT,
            BODY,
            REPORT_NAME,
        )

        date_ = data_interval_end - duration(1)
        file_template = os.path.join(
            AUTO_REPORTS_ROOT,
            REPORT_NAME,
            date_.strftime("%Y%m"),
            date_.strftime("%Y-%m-%d"),
            "BY_MO",
            "_zipped",
            "{mo_}.zip",
        )
        return {
            mo_: {
                'attachments': [file_template.format(mo_=mo_)],
                'subject': SUBJECT.format(date=date_.strftime('%d.%m.%Y')),
                'body_text': BODY.format(date=date_.strftime('%d.%m.%Y')),
                # 'emails': ['kulikovdy@zdrav.mos.ru']
                'emails': [Email.get_mo_mail(mo_name=mo_)],
            }
            for mo_ in MO_LIST
        }

    (
        ExternalTaskSensor(
            task_id="wait_report",
            external_dag_id="daily_report_ref_activ",
            timeout=duration(hours=15),
            mode="reschedule",
            poke_interval=duration(minutes=5),
            dag=dag,
        )
        >> PythonOperator(task_id='mailing_config', python_callable=mailing_config)
        >> send_mails_by_mo(MO_LIST)
    )
```
