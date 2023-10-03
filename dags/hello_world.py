from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id="hello_world",
    start_date=pendulum.today("UTC").add(days=-14),
    description="This DAG wil print 'Hello' and 'World'.",
    schedule="@daily",
) as dag:

    hello = BashOperator(
        task_id="Hello",
        bash_command="echo 'Hello'",
    )

    world = PythonOperator(
        task_id="World",
        python_callable=lambda: print("World")
    )

    hello >> world