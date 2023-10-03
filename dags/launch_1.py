from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
import pendulum

with DAG(
    dag_id="launch_1",
    start_date=pendulum.today("UTC").add(days=-14),
    description="This DAG wil launch a rocket.",
    schedule="@daily",
) as dag:

    build_stage = [EmptyOperator(task_id=f"build_stage_{i}", dag=dag) for i in range(3)]

    material = EmptyOperator(task_id="procure_rocket_material", dag=dag)
    fuel = EmptyOperator(task_id="procure_fuel", dag=dag)
    launch = EmptyOperator(task_id="launch", dag=dag)

    material >> build_stage >> launch
    fuel>> build_stage[2]