#!/usr/bin/env python
# coding: utf-8

# In[28]:


from datetime import timedelta
from datetime import datetime
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


# In[32]:


args = {
    'owner': 'Airflow',
    'start_date': datetime(2020,1,1),
    'retries': 1,
}

dag = DAG(
    dag_id='test_dag',
    default_args=args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=60),
)


# In[30]:


task1 = DummyOperator(
    task_id='task1',
    dag=dag,
)
task2 = DummyOperator(
    task_id='task2',
    dag=dag,
)
task3 = DummyOperator(
    task_id='task3',
    dag=dag,
)

task4 = DummyOperator(
    task_id='task4',
    dag=dag,
)   
    
task5 = DummyOperator(
    task_id='task5',
    dag=dag,
)
task1 >> task2 >> task3 >> task5
task2 >> task4 >> task5


# In[ ]:




