from datetime import datetime
import requests
from airflow.decorators import dag, task
from google.cloud import storage

@dag(
    dag_id='dzik_scrape_legacy',
    tags=['dzik'],
    default_args={'owner': 'JW'},
    schedule='40 */4 * * *',
    catchup=False,
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2030, 1, 1),
)

def dag_creator():

    bucket_name = 'dzik-scrape'

    @task
    def list_blobs(bucket_name):
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(bucket_name)

        file_names = [blob.name for blob in blobs]

        if not file_names:
            print('No files, starting from 0')
            starter_point = 0
        else:
            file_names.sort(key=lambda x: int(x[:-4]))
            starter_point = int(file_names[-1][:-4])

        print('Starter point: ', starter_point)
        return starter_point

    @task
    def image_puller(starter_point):
        failed_attempts = 0
        counter = starter_point + 1
        discord_package = []

        while failed_attempts < 500:
            for extension in ['jpg', 'png']:
                url = f'https://wkdzik.pl/userdata/public/gfx/{counter}/mango-szare.{extension}'
                file_name = f'{counter}.jpg'
                response = requests.get(url)
                if response.status_code == 200:
                    storage_client = storage.Client()
                    contents = response.content
                    bucket = storage_client.bucket(bucket_name)
                    blob = bucket.blob(file_name)
                    blob.upload_from_string(contents, content_type='image')
                    print('Downloaded image: ', counter, '[', extension, ']')

                    discord_package.append(file_name)

                    failed_attempts = 0
                    counter += 1
                    break
                else:
                    print('No image in:', counter, '[', extension, ']', '| Failed attempts: ', failed_attempts)
            else:
                failed_attempts += 1
                counter += 1

        print(f'End of downloading after {failed_attempts} failed attempts.')
        return discord_package

    @task
    def send_image_message(discord_package):

        webhook_url = 'https://discord.com/api/webhooks/xxx/xxx'

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        for file in discord_package:
            try:
                blob = bucket.blob(file)
                image_bytes = blob.download_as_bytes()

                files = {
                    'file': (file, image_bytes)
                }
                data = {
                    'content': f'[#] {file}'
                }

                response = requests.post(webhook_url, data=data, files=files)

                if response.status_code == 204:
                    print(f'Wysłano {file} na Discorda.')
                else:
                    print(f'Błąd wysyłania {file}: {response.status_code} {response.text}')
            except:
                print('ERROR')


    starter_point = list_blobs(bucket_name)
    image_puller_task = image_puller(starter_point)
    send_image_message_task = send_image_message(image_puller_task)

dag = dag_creator()