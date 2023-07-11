import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime

def get_data_mongo(ti, **context):
    try:
        hook = MongoHook(conn_id='mongo-init')
        client = hook.get_conn()
        db = client.airflow
        collection = db.music
        print(f"Connected to MongoDB - {client.server_info()}")

        data = pd.DataFrame(list(collection.find()))

        data = data.drop(columns=["_id"])
        print("column id", data.head(5))
        data = data.head(600)
        data_json = data.to_json(orient='records')
        print("data_json", data_json)

        ti.xcom_push(key='result-mongo', value=data_json)

        return data_json

    except Exception as e:
        print(f"Error getting data from MongoDB -- {e}")
        raise e


def join_data(ti, **context):
    result_api = context["result-api"]
    result_mongo = context["result-mongo"]

    # Cargar los datos del API y MongoDB en DataFrames
    api_data = pd.read_json(result_api)
    mongo_data = pd.read_json(result_mongo)

    # Añadir columnas faltantes al DataFrame del API
    api_data['duration_ms'] = None

    # Obtener el último ID de la API
    ultimo_id_api = api_data['id'].max()

    # Filtrar los datos de MongoDB a partir del último ID de la API
    mongo_filtrado_id = mongo_data[mongo_data['id'] > ultimo_id_api]

    # Concatenar los DataFrames filtrados
    data_unida = pd.concat([api_data, mongo_filtrado_id], ignore_index=True)

    # Convertir el DataFrame resultante en formato JSON
    json_data = data_unida.to_json(orient='records')

    # Guardar el resultado en el contexto
    ti.xcom_push(key='joined_data', value=json_data)

    return json_data


def transform_data(ti, **context):

    data_unida = context["joined_data"]
    data_json = pd.read_json(data_unida)

    # Seleccionar columnas necesarias y renombrar
    data_selec = data_json.loc[:, ['id', 'artist', 'name', 'year']]
    data_transformada = data_selec.rename(columns={'name': 'song'})

    # Convertir el DataFrame transformado en formato JSON
    data_final = data_transformada.to_json(orient='records')

    # Guardar el resultado en el contexto
    ti.xcom_push(key='transformed_data', value=data_final)

    return data_final


def upload_data_mongo(ti, **context):
    try:
        hook = MongoHook(conn_id='mongo-init')
        client = hook.get_conn()
        db = client.airflowEnd
        collection = db.musicDef
        print(f"Connected to MongoDB - {client.server_info()}")

        data = context["transformed_data"]
        # # data = context["result"]

        items = pd.read_json(data)

        for _, item in items.iterrows():
            collection.insert_one(dict(item))

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")
        raise e


with DAG(
        dag_id="DATA-EJERCICIO",
        description="Obtener los datos desde una API y mongoDB, y unirlos en otro mongoDB",
        schedule_interval="0/5 12 * * *", # Cada 5 minutos de 0 a 59 pasadas las 12 horas.
        start_date=datetime(2023, 7, 11),
        end_date=datetime(2023, 10, 11),
        default_args={"depends_on_past": True},
        max_active_runs=1) as dag:

    t1 = SimpleHttpOperator(task_id="API-data",
                            method="GET",
                            http_conn_id="music-api",
                            headers={"Content-Type": "application/json"},
                            do_xcom_push=True)

    t2 = PythonOperator(task_id="MongoDB-data",
                        python_callable=get_data_mongo,
                        # op_kwargs={"result": t1.output},
                        do_xcom_push=True)

    t3 = PythonOperator(task_id="join-data",
                        provide_context=True,
                        python_callable=join_data,
                        op_kwargs={"result-mongo": t2.output,
                                   "result-api": t1.output},
                        do_xcom_push=True)

    t4 = PythonOperator(task_id='transform-data',
                        provide_context=True,
                        python_callable=transform_data,
                        op_kwargs={"joined_data": t3.output},
                        do_xcom_push=True)

    t5 = PythonOperator(task_id='upload-mongodb',
                        provide_context=True,
                        python_callable=upload_data_mongo,
                        op_kwargs={"transformed_data": t4.output},
                        dag=dag)
    
    [t1, t2] >> t3 >> t4 >> t5
