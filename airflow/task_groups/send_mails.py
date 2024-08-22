from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models import DagRun
from airflow.utils.state import TaskInstanceState
from typing import Iterable, Literal
from pendulum import duration


def send_mails_by_mo(mo_names: list[str], group_id: str = 'send_files_by_mo'):
    with TaskGroup(group_id=group_id) as group:
        """формирует таск группу для рассылки файлов по мо. выше по течению ожидает таску с id 
        mailing_config, которая возвращает словарь с параметрами для рассылки вида
        {
            'mo_name': {
                    'attachments': list[str],
                    'subject': str,
                    'body_text': str,
                    'emails': list[str],
                },
        }
        """
      
        formatted_names = tuple(mo_name.replace(" ", "") for mo_name in mo_names)
        check_task_ids = tuple(f"check_file_for_{name}" for name in formatted_names)
        send_task_ids = tuple(f"send_mail_to_{name}" for name in formatted_names)

        def complete(dag_run: DagRun = None) -> str:
            """Возвращает читаемый отчёт об отправке"""
            from collections import Counter
          
            def iter_states(
                ids: Iterable[str],
            ) -> Iterable[
                Literal[
                    TaskInstanceState.SUCCESS,
                    TaskInstanceState.FAILED,
                    TaskInstanceState.SKIPPED,
                    TaskInstanceState.UPSTREAM_FAILED,
                ]
            ]:
                return (dag_run.get_task_instance(id_).state for id_ in ids)

            print(check_task_ids)
            print(send_task_ids)
            check_counts = Counter(
                iter_states(group_id + '.' + task_id for task_id in check_task_ids)
            )
            send_counts = Counter(
                iter_states(group_id + '.' + task_id for task_id in send_task_ids)
            )

            failed_sendings = send_counts[TaskInstanceState.FAILED]
            sent_emails = send_counts[TaskInstanceState.SUCCESS]
            absent_reports = send_counts[TaskInstanceState.SKIPPED]
            failed_checkings = check_counts[TaskInstanceState.FAILED]

            return (
                f"dag_id: #{dag_run.dag_id}\n"
                f"Успешно отправлено в {sent_emails}/{len(send_task_ids)} МО.\n"
                + (
                    (f"Упала {failed_sendings} отправка.\n" if failed_sendings else "")
                    if sent_emails
                    else "Не было отправлено ни одного письма.\n"
                )
                + (
                    f"Отсутствуют отчёты для {absent_reports} МО.\n"
                    if absent_reports
                    else ""
                )
                + (
                    f"Ошибка проверки отчётов для {failed_checkings}.\n"
                    if failed_checkings
                    else ""
                ),
            )

        def check_file(
            mo_name: str,
            ti: TaskInstance = None,
        ):
            from pathlib import Path
            from airflow.exceptions import AirflowSkipException

            mailing_config = ti.xcom_pull('mailing_config')
            for filepath in mailing_config[mo_name]['attachments']:
                print(filepath)
                if not Path(filepath).exists():
                    raise AirflowSkipException(
                        f"Не найден файл для отправки в МО {filepath}."
                    )

        def send_mail(mo_name: str, ti: TaskInstance = None):
            from removed_for_confidentiality import MailConnector

            mailing_config = ti.xcom_pull('mailing_config')
            MailConnector().send(**mailing_config[mo_name])

        complete_ = PythonOperator(
            task_id='complete', python_callable=complete, trigger_rule='all_done'
        )
        for mo_name, check_id, send_id in zip(mo_names, check_task_ids, send_task_ids):
            check_file_ = PythonOperator(
                task_id=check_id,
                python_callable=check_file,
                op_args=[mo_name],
            )
            send_mail_ = PythonOperator(
                task_id=send_id,
                python_callable=send_mail,
                op_args=[mo_name],
                execution_timeout=duration(minutes=5),
            )

            check_file_ >> send_mail_ >> complete_

    return group
