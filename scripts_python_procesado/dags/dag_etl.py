from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from src import steps

default_args = {
    'owner': 'OPI',
    'start_date': datetime(2020, 4, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_tortas_tamal_inc',
    default_args=default_args,
    catchup=False,
    description='This task will generate a start',
    schedule_interval="0 0 1 * *",
)

crearBaseDatos = PythonOperator(
    task_id="crearBaseDatos",
    python_callable=steps.crearBaseDatos,
    dag=dag
)

dim_region = PythonOperator(
    task_id="dim_region",
    python_callable=steps.agregarDimRegion,
    dag=dag
)

dim_producto = PythonOperator(
    task_id="dim_producto",
    python_callable=steps.agregarDimProducto,
    dag=dag
)



dim_tiempo = PythonOperator(
    task_id="dim_tiempo",
    python_callable=steps.agregarDimTiempo,
    op_kwargs={"inicio":2017,"fin":2021},
    dag=dag
)

generarMetricasPorMes = PythonOperator(
    task_id="generarMetricasPorMes",
    python_callable=steps.generarMetricasPorMes,
    op_kwargs={"fechaActualAAAAMM":"201902"},
    dag=dag
)

generarMetricasPorMes2 = PythonOperator(
    task_id="generarMetricasPorMes2",
    python_callable=steps.generarMetricasPorMes,
    op_kwargs={"fechaActualAAAAMM":"201903"},
    dag=dag
)

generarMetricasPorMes3 = PythonOperator(
    task_id="generarMetricasPorMes3",
    python_callable=steps.generarMetricasPorMes,
    op_kwargs={"fechaActualAAAAMM":"201904"},
    dag=dag
)

generarMetricasPorMes4 = PythonOperator(
    task_id="generarMetricasPorMes4",
    python_callable=steps.generarMetricasPorMes,
    op_kwargs={"fechaActualAAAAMM":"201905"},
    dag=dag
)


crearBaseDatos >> dim_region

crearBaseDatos >> dim_producto

crearBaseDatos >> dim_tiempo


generarMetricasPorMes << dim_tiempo

generarMetricasPorMes << dim_producto

generarMetricasPorMes << dim_region


generarMetricasPorMes >> generarMetricasPorMes2

generarMetricasPorMes2 >> generarMetricasPorMes3

generarMetricasPorMes3 >> generarMetricasPorMes4