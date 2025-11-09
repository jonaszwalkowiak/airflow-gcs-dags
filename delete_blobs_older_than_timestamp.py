from airflow.sdk import dag, task, Variable
from google.cloud import storage
from datetime import datetime, timezone

@dag(
    dag_id='delete-blobs-older-than-timestamp',
    tags=['data-sedum'],
    default_args={"owner": "JW"},
    schedule=None
)

def dag_creator():

    @task
    def delete_files_from_bucket():

        bucket_name = Variable.get("data-sedum-test-bucket")

        # Inicjacja sesji klienta w GCS + odczyt bucketa ze zmiennej
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Ustawienie timestampa
        cutoff = datetime(2025, 11, 4, 18, 15, 9, 884, tzinfo=timezone.utc)
        print(f"Wybrany TIMESTAMP: {cutoff}")

        # Usuwanie iteracyjne
        for blob in bucket.list_blobs():
            if blob.updated < cutoff:
                print(f"Usuwam: {blob.name} | Data utworzenia pliku: {blob.time_created}")
                blob.delete()

    delete_files_from_bucket()

dag = dag_creator()
