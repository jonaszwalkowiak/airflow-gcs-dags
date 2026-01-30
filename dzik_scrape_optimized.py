from datetime import datetime
import requests
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import Variable, dag, task

@dag(
    dag_id="test",
    tags=["test"],
    default_args={"owner": "JW"},
    # schedule=None,
    schedule="20 */4 * * *",
    catchup=False,
    start_date=datetime(2025, 7, 20),
    end_date=datetime(2030, 1, 1),
    max_active_runs=1,
)

def dag_creator():

    @task
    def generate_endpoints():
        last_value = int(Variable.get("dzik_scrape_last_value"))
        extensions = ["jpg", "png"]
        endpoints = [
            f"https://wkdzik.pl/userdata/public/gfx/{v}/mango-szare.{e}"
            for v in range(last_value, last_value + 500)
            for e in extensions
        ]

        return endpoints

    @task
    def image_puller(endpoints):
        gcs_hook = GCSHook(gcp_conn_id="gcp_conn_id")
        new_images_urls = []
        for url in endpoints:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                print(f"{response} - Pobieram obrazek z: {url}")
                filename = url[38:-16] + url[-4:]
                gcs_hook.upload(
                    bucket_name="dzik-scrape-test",
                    object_name=f"{filename}",
                    data=response.content,
                    mime_type=response.headers.get("Content-Type", "image/jpeg"),
                )
                new_images_urls.append(f"{filename}")
            else:
                print(f"{response} - Nie ma grafiki w: {url}")

        if not new_images_urls:
            raise AirflowSkipException(
                "Brak nowych obrazków w tym zakresie. Kończę pracę."
            )

        return new_images_urls

    @task
    def discord_agent(new_images_urls):
        gcs_hook = GCSHook(gcp_conn_id="gcp_conn_id")
        url = Variable.get("discord_webhook")

        for file in new_images_urls:
            image = gcs_hook.download_as_byte_array(
                bucket_name="dzik-scrape-test", object_name=f"{file}"
            )

            response = requests.post(
                url=url,
                files={"file": (file, image, "image/jpeg")},
                data={"content": f"[# NEW #] {file}"},
            )

            if response.status_code == 200:
                print(f"Wysłano {file} na Discorda.")
            else:
                print(f"Błąd wysyłania {file}: {response.status_code} {response.text}")

    @task
    def update_last_value(new_images_urls):
        if not new_images_urls:
            print("Nie ma nowych obrazków.")
        else:
            max_value_string = new_images_urls[-1]
            max_value = int(max_value_string[:-4])
            print(f"Ostatni zapisany obrazek: {max_value}")
            Variable.set("dzik_scrape_last_value", str(max_value + 1))

    endpoints_list = generate_endpoints()
    list_of_gcs_files = image_puller(endpoints_list)
    discord_agent(list_of_gcs_files)
    update_last_value(list_of_gcs_files)

dag = dag_creator()