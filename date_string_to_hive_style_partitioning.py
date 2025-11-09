from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

@dag(
    dag_id='date_string_to_hive_style_partitioning',
    tags=['data-sedum'],
    default_args={"owner": "JW"},
    schedule=None,
    catchup=False,
    start_date=datetime(2025, 7, 20),
    end_date=datetime(2030, 1, 1),
    max_active_runs=1
)

def dag_creator():

    source_bucket="nyc-fhv-active"
    destination_bucket="nyc-fhv"

    list_blobs_from_bucket = GCSListObjectsOperator(
        task_id="list_blobs_from_bucket",
        gcp_conn_id="gcp_conn_id",
        bucket=source_bucket
    )

    @task
    def convert_list_to_hive(blobs_in_bucket):
        print(blobs_in_bucket)
        mapped_args = []
        for blob in blobs_in_bucket:
            # Logika parsowania nazwy pliku
            new_blob_year = blob[:4]
            new_blob_month = blob[5:7]
            new_blob_day = blob[8:10]
            print(blob + " ### " +new_blob_year + " " + new_blob_month + " " + new_blob_day)
            new_blob = f"fhv_active_{new_blob_year}{new_blob_month}{new_blob_day}T000000.csv"
            print(new_blob)
            final_path = f"bronze/fhv-active/year={new_blob_year}/month={new_blob_month}/day={new_blob_day}/{new_blob}" 
            print(final_path)
            
            # Para w jednym obiekcie: źródło + cel
            mapped_args.append({
                "source_object": blob,
                "destination_object": final_path
                })
        
        return mapped_args

    # Dynamic Task Mapping
    converted_paths = convert_list_to_hive(list_blobs_from_bucket.output)

    upload_to_hive = GCSToGCSOperator.partial(
        task_id="upload_to_hive",
        gcp_conn_id="gcp_conn_id",
        source_bucket=source_bucket,
        destination_bucket=destination_bucket,
        exact_match=True,
    ).expand_kwargs(
        converted_paths
    )

    list_blobs_from_bucket >> converted_paths >> upload_to_hive

dag = dag_creator()