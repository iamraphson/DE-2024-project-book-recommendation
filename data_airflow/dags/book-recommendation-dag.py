import os
import datetime as dt
import logging;
import pandas as pd
import pyarrow.fs as pafs
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BOOK_RECOMMENDATION_BUCKET = os.environ.get('GCP_BOOK_RECOMMENDATION_BUCKET')
BOOK_RECOMMENDATION_WH_EXT = os.environ.get('GCP_BOOK_RECOMMENDATION_WH_EXT_DATASET')
BOOK_RECOMMENDATION_WH = os.environ.get('GCP_BOOK_RECOMMENDATION_WH_DATASET')
GCP_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

dataset_download_path = f'{AIRFLOW_HOME}/book-recommendation-dataset/'
parquet_store_path = f'{dataset_download_path}pq/'

gcs_store_dir = '/book-recommendation-pq'
gcs_pq_store_path = f'{BOOK_RECOMMENDATION_BUCKET}{gcs_store_dir}'

book_recommendation_datasets = ['books', 'ratings', 'users']

book_dtype = {
    'isbn': pd.StringDtype(),
    'book_title': pd.StringDtype(),
    'book_author': pd.StringDtype(),
    'year_of_publication': pd.Int64Dtype(),
    'publisher': pd.StringDtype(),
}

user_dtype = {
    'user_id': pd.Int64Dtype(),
    'age': pd.Int64Dtype(),
    'location': pd.StringDtype()
}

rating_dtype = {
    'user_id': pd.Int64Dtype(),
    'isbn': pd.StringDtype(),
    'rating': pd.Int64Dtype(),
}

def do_clean_to_parquet():
    if not os.path.exists(parquet_store_path):
        os.makedirs(parquet_store_path)

    for filename in os.listdir(dataset_download_path):
        if filename.endswith('.csv'):
            dataset_df = pd.read_csv(f'{dataset_download_path}{filename}')

            if filename.startswith('Books'):
                dataset_df = dataset_df.drop(columns=[
                    'Image-URL-S',
                    'Image-URL-M',
                    'Image-URL-L'
                ], axis='columns')

                dataset_df = dataset_df.rename(mapper={
                    'Book-Title': 'book_title',
                    'Book-Author': 'book_author',
                    'Year-Of-Publication': 'year_of_publication',
                    'Publisher': 'publisher',
                    'ISBN': 'isbn'
                }, axis='columns')

                dataset_df['year_of_publication'] = pd.to_numeric(dataset_df['year_of_publication'], errors='coerce')
                dataset_df = dataset_df.dropna(subset=['year_of_publication'])
                dataset_df = dataset_df.astype(book_dtype)
            elif filename.startswith('Users'):
                dataset_df = dataset_df.rename(mapper={
                    'User-ID': 'user_id',
                    'Age': 'age',
                    'Location': 'location'
                }, axis='columns')

                dataset_df = dataset_df.astype(user_dtype)

                dataset_df['location_data'] = dataset_df['location'].apply(lambda x: [x.strip() for x in x.split(',')]) #split by a comma and trim
                dataset_df['location_data'] = dataset_df['location_data'].apply(lambda values: [val for val in reversed(values) if val is not None][:3][::-1])

                dataset_df[['city', 'state', 'country']] = pd.DataFrame(dataset_df['location_data'].tolist())
                dataset_df.drop(columns=['location', 'location_data'], inplace=True)
                dataset_df['age'].fillna(0, inplace=True)
            elif filename.startswith('Ratings'):
                dataset_df = dataset_df.rename(mapper={
                    'User-ID': 'user_id',
                    'ISBN': 'isbn',
                    'Book-Rating': 'rating'
                }, axis='columns')

                dataset_df.astype(rating_dtype)

                dataset_df['rating'].fillna(0, inplace=True)
            else:
                continue


            print('dataset_df.columns', dataset_df.columns)
            parquet_filename = filename.lower().replace('.csv', '.parquet')
            parquet_loc = f'{parquet_store_path}{parquet_filename}'

            dataset_df.reset_index(drop=True, inplace=True)
            dataset_df.to_parquet(parquet_loc)

            logging.info('Done cleaning up!')


def do_upload_pq_to_gcs():
    gcs = pafs.GcsFileSystem()
    dir_info = gcs.get_file_info(gcs_pq_store_path)
    if dir_info.type != pafs.FileType.NotFound:
        gcs.delete_dir(gcs_pq_store_path)

    gcs.create_dir(gcs_pq_store_path)
    pafs.copy_files(
        source=parquet_store_path,
        destination=gcs_pq_store_path,
        destination_filesystem=gcs
    )

    logging.info('Copied parquet to gsc')


default_args = {
    'owner': 'iamraphson',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(
    'Book-Recommendation-DAG',
    default_args=default_args,
    description='DAG for book recommendation dataset',
    tags=['Book Recommendation'],
    user_defined_macros={
        'BOOK_RECOMMENDATION_WH_EXT': BOOK_RECOMMENDATION_WH_EXT,
        'BOOK_RECOMMENDATION_WH': BOOK_RECOMMENDATION_WH
    }
) as dag:
    install_pip_packages_task = BashOperator(
        task_id='install_pip_packages_task',
        bash_command='pip install --user kaggle'
    )

    pulldown_dataset_task = BashOperator(
        task_id='pulldown_dataset_task',
        bash_command=f'kaggle datasets download arashnic/book-recommendation-dataset --path {dataset_download_path} --unzip'
    )

    do_clean_to_parquet_task = PythonOperator(
        task_id='do_clean_to_parquet_task',
        python_callable=do_clean_to_parquet
    )

    do_upload_pq_to_gcs_task = PythonOperator(
        task_id='do_upload_pq_to_gcs_task',
        python_callable=do_upload_pq_to_gcs
    )

    with TaskGroup('create-external-table-group-tasks') as create_external_table_group_task:
        for dataset in book_recommendation_datasets:
            BigQueryCreateExternalTableOperator(
                 task_id=f'bq_external_{dataset}_table_task',
                 table_resource={
                     'tableReference': {
                         'projectId': GCP_PROJECT_ID,
                         'datasetId': BOOK_RECOMMENDATION_WH,
                         'tableId': dataset,
                     },
                     'externalDataConfiguration': {
                         'autodetect': True,
                         'sourceFormat': 'PARQUET',
                         'sourceUris': [f'gs://{gcs_pq_store_path}/{dataset}.parquet'],
                     },
                 },
             )

    create_table_partitions_task = BigQueryInsertJobOperator(
        task_id = 'create_table_partitions_task',
        configuration={
            'query': {
                'query': "{% include 'sql/load-dwh.sql' %}",
                'useLegacySql': False,
            }
        }
    )


    clean_up_dataset_store_task = BashOperator(
        task_id='clean_up_dataset_store_task',
        bash_command=f"rm -rf {dataset_download_path}"
    )

    uninstall_pip_packge_task = BashOperator(
        task_id='uninstall_pip_package_task',
        bash_command=f"pip uninstall --yes kaggle"
    )

    install_pip_packages_task.set_downstream(pulldown_dataset_task)
    pulldown_dataset_task.set_downstream(do_clean_to_parquet_task)
    do_clean_to_parquet_task.set_downstream(do_upload_pq_to_gcs_task)
    do_upload_pq_to_gcs_task.set_downstream(create_external_table_group_task)
    create_external_table_group_task.set_downstream(create_table_partitions_task)
    create_table_partitions_task.set_downstream(clean_up_dataset_store_task)
    create_table_partitions_task.set_downstream(uninstall_pip_packge_task)
