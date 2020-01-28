#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import timedelta
from datetime import datetime
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

def _print_get_weekday(execution_date, **context):
    return print(execution_date.strftime("%a"))

def _get_weekday(execution_date, **context):
    return execution_date.strftime("%a")

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(7),
}

with DAG(
    dag_id='exercise-templating',
    default_args=args,
    schedule_interval='@daily',
) as dag:


print_weekday = PythonOperator(
    task_id="print_weekday",
    python_callable=__print_get_weekday,
    provide_context=True,
    dag=dag,
)

branching = BranchPythonOperator(
    task_id="branching", 
    python_callable=_get_weekday, 
    provide_context=True,
    dag=dag
)


mails = ["email_a", "email_b", "email_c"]
for mail in mails:
    branching >> DummyOperator(task_id=mail, dag=dag)
    
final_task = DummyOperator(
    task_id="join",
    trigger_rule="none_failed"
)

print_weekday >> branching >> email_a >> final_task
branching >> email_b >> final_task
branching >> email_c >> final_task

