from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

import logging




logger=logging.getLogger(__name__)

default_args= {
    'owner':'utkarsh',
    'depends_on_past':False,
    'start_date':datetime(2025,6,17),
   # 'execution_timeout':timedelta(minutes=120),
    'max_active_runs':1,

}

def _train_model(**context):
    """Airflow wrapper for training task """

    pass

with DAG(

    dag_id='fraud_detection_training',
    default_args='default_args',
    description='Fraud Detection model training pipeline',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['fraud','ML']

) as dag:
    validate_environment=BashOperator(
        task_id='validate_environment',
        bash_command =
        '''
        echo "Validating env..."
        test -f /app/config.yaml &&
        test -f /app/.env &&
        echo 'Environment valid !"
        '''

    )

    training_task =PythonOperator(
        task_id='execute_training',
        python_callable='train_model',
        provide_context=True,

    )


    cleanup_task =BashOperator(
        task_id='cleanup resources',
        bash_command= 'rm -f/app/tmp/*.pkl',
        trigger_rule='all_don',
    )

    validate_environment >>training_task >>cleanup_task

    #Docs
    dag.doc_md = '''
    ## Fraud Detection Training Pipeline
    
    Daily training of fraud detection model using ;
    -Transaction data from Kafka
    -XGBoost classifier with precision optimization
    
    
    
    '''


