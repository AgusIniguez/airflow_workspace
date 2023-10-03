from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum

def _print_context(**kwargs):
    print(f"This code was executed at {kwargs['execution_date']}")
    print(f"Programmed execution date is {kwargs['ds']}")

with DAG(
    dag_id="context_1",
    start_date=pendulum.today("UTC").add(days=-14),
    description="This DAG wil print the context.",
    schedule="@daily",
) as dag:

    hello = BashOperator(
        task_id="context_bash",
        bash_command="echo '{{ task.task_id }} is running in the {{ dag.dag_id }} pipeline'",
    )

    world = PythonOperator(
        task_id="context_python",
        python_callable=_print_context,
    )

    # hello >> world