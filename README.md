# Airflow DAGs - Google Cloud Storage

Projekt przedstawia przykładowe DAGi w Airflow dedykowane do operacji na Google Cloud Storage (GCS), z naciskiem na realne scenariusze operacyjne i dobre praktyki orkiestracji.


Druga część zawiera metody i kroki konfiguracji integracji Apache Airflow z Google Cloud Storage poprzez przygotowanie środowiska, dodanie odpowiednich zmiennych połączeń, pakietów oraz utworzenie konta serwisowego w Cloud IAM.

---

# Lista DAGów



### ■ [date_string_to_hive_style_partitioning.py](https://github.com/jonaszwalkowiak/airflow-gcs-dags/blob/master/date_string_to_hive_style_partitioning.py)

<details>
<summary>Kliknij, aby zobaczyć szczegółowy opis DAGa</summary>

#### Opis: Konwersja nazwy obiektów z formatu `Date String` na `Hive-Style Partitioning`

**Cel:** Zmiana nazwy wszystkich obiektów z bucketa z formatu Date String `YYYY-MM-DD` na Hive-Style Partitioning `/year=2026/month=01/day=20/hour=12/`.

**Logika Potoku:**
1.  **Listowanie istniejących obiektów w buckecie**

    Wykorzystanie operatora `GCSListObjectsOperator` do pobrania listy obiektów ze wskazanego bucketa.
2.  **Edycja nazwy i stworzenie docelowej ścieżki**
    * Slicing nazwy, aby wydobyć zmienne w dacie.
    * Stworzenie docelowej ścieżki dla nowego BLOB.
    * Sparowanie oryginalnej nazwy z nową w liście słowników (uniknięcie problemu "Iloczynu kartezjańskiego" powstającego przy użyciu metody `.expand`).
3.  **Upload (kopia) obiektów według stworzonego słownika**

    Wykorzystanie operatora `GCSToGCSOperator` z metodami `.partial` oraz `.expand_kwargs`.

#### Zagadnienia:
* Hive-Style Partitioning: standard w Big Data | dedykowany format pod BigQuery i PySpark | partycjonowanie (optymalizacja kosztów)
* [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html#task-generated-mapping): skalowalność | mapowane Task Index (atomowość, licznik)
* Operatory: dedykowane operatory [GCSListObjectsOperator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/gcs/index.html#airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator) oraz [GCSToGCSOperator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/transfers/gcs_to_gcs/index.html) w połączeniu z `Vanilla Python`
</details>



### ■ [delete_blobs_older_than_timestamp.py](https://github.com/jonaszwalkowiak/airflow-gcs-dags/blob/master/delete_blobs_older_than_timestamp.py)

<details>
<summary>Kliknij, aby zobaczyć szczegółowy opis DAGa</summary>

#### Opis: Usuwanie BLOBs z bucketa na podstawie daty dodania

**Cel:** Usunięcie wszystkich elementów starszych (według daty dodania) ze wskazanego bucketa, niż zdefiniowany timestamp. Brak dedykowanego operatora Airflow dla takiej czynności - stąd użycie `Vanilla Python` z biblioteką `google.cloud`. Koncept zmiennych w Airflow.

#### Zagadnienia:
* Zmienne w Airflow - [Dokumentacja Airflow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html)
* Lista metadanych obiektów - [Dokumentacja GCP](https://docs.cloud.google.com/storage/docs/viewing-editing-metadata)
* Usuwanie obiektów z bucketów - [Dokumentacja GCP](https://docs.cloud.google.com/storage/docs/deleting-objects#storage-delete-object-python)
</details>



### ■ [dzik_scrape_legacy.py](https://github.com/jonaszwalkowiak/airflow-gcs-dags/blob/master/dzik_scrape_legacy.py) | [dzik_scrape_optimized.py](https://github.com/jonaszwalkowiak/airflow-gcs-dags/blob/master/dzik_scrape_optimized.py)

<details>
<summary>Kliknij, aby zobaczyć szczegółowy opis DAGa</summary>

#### Opis: Pobranie niewidocznych zdjęć ze sklepu online na podstawie wzoru URL

**Cel:** Sprawdzenie wygenerowanej listy linków czy zawierają oczekiwany zasób - grafikę. Jeżeli warunek jest prawdziwy, następuje zapis nowego zasobu do GCS oraz wysłanie go w formie załącznika na komunikator (Discord). Cała logika zadania jest ustawiona w taki sposób, aby była kontynuowana w pętli i znajdowała nowe assety.

**Porównanie rozwiązania `legacy` z `optimized`:**
* Zmiana sposobu połączenia z GCS ze `zmiennej środowiskowej` na `Airflow Connections` (conn_id) - bezpieczeństwo, standaryzacja
* Zmiana podejścia do uzyskiwania wartości punktu startowego - rezygnacja z listowania całego bucketa oraz operacji na danych (koszt), na rzecz `Airflow Variable`
* Użycie dedykowanych rozwiązań (podejście Airflow-Native) - Hooks, Variables, AirflowSkipException
* Oddzielenie konfiguracji od kodu

#### Zagadnienia:
* GCSHook - [Dokumentacja Airflow](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/hooks/gcs/index.html)
* AirflowSkipException - [Dokumentacja Airflow](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/exceptions/index.html)
* Operacje na zmiennych Airflow - [Dokumentacja Astro](https://www.astronomer.io/docs/learn/airflow-variables)
</details>

---

# Połączenie (GCS)



### [Connection] Airflow ---> Google Cloud Storage

Konfiguracja połączenia | Cloud IAM

1. **Google Cloud Platform (IAM)** - Service Account

    Utworzenie konta serwisowego z prawami Storage Admin (autoryzacja przez JSON key)

2. **Airflow Connections** - Dodanie nowego połączenia

    Wybranie odpowiedniego typu połączenia i uzupełnienie szyfrowanych pól ze zmiennymi - w tym dane z pliku JSON key.

    Wymagany format Airflow: `"keyfile_dict": {}`

3. **Użycie** - wykorzystanie nowo powstałego połączenia na podstawie jego unikalnego ID w DAGu - dedykowane operatory i hooki.

    ```
    gcp_conn_id
    ```



### [JSON key] Airflow ---> Google Cloud Storage

Konfiguracja środowiska i połączenia | Cloud IAM

1. **Instalacja paczki** - interakcja/zarządzanie Google Cloud Storage

    Dodanie `google-cloud-storage` do [środowiska Airflow](https://github.com/jonaszwalkowiak/astro-airflow3.git) - plik `requirements.txt`

2. **Google Cloud Platform (IAM)** - Service Account

* Utworzenie konta serwisowego z prawami Storage Admin (autoryzacja przez JSON key)
    
* Transfer pliku na VPS do folderu `./secrets/`

3. **Dodanie zmiennej środowiskowej** - plik `.env`

    ```yaml
    GOOGLE_APPLICATION_CREDENTIALS = /usr/local/airflow/secrets/gcp-storage-admin.json
    ```

    Zmienna środowiskowa - credentiale do GCS będą dostępne z poziomu całego środowiska Airflow. Nie trzeba wskazywać poświadczeń chociażby w DAGach.

4. **Restart** środowiska Airflow

    ```bash
    astro dev restart
    ```