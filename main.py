# --- 1. Autentikasi Google Cloud ---
from google.colab import auth
auth.authenticate_user()

# --- 2. Import Library ---
import requests
import pandas as pd
from google.cloud import bigquery
import urllib3

# Disable SSL warning karena URL memakai verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():

    # --- 3. URL API ---
    url = (
        "https://43.218.20.129:447/dev/get_cds.php"
        "?p_cek=x"
        "&p_cds=ZA_HENDITESTV"
        "&p_user=AO000018"
        "&p_pass=@1Nusantara"
        "&p_filter=tahun;eq;%272025%27;and;periode;eq;%2712%27;"
    )

    # --- 4. Ambil API ---
    print("Fetching data from API...")
    response = requests.get(url, verify=False)

    # Validasi status HTTP
    if response.status_code != 200:
        print("ERROR fetching API:", response.text)
        return

    data = response.json()

    # --- 5. Convert ke DataFrame ---
    if not data:
        print("API returned empty data!")
        return

    df = pd.DataFrame(data)
    print(f"Rows fetched: {len(df)}")

    # --- 6. BigQuery Setting ---
    project_id = "real-time-trans-sap"
    dataset_id = "SAP"
    table_id   = "Bkm"

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # --- 7. Load Data ke BigQuery ---
    print("Uploading to BigQuery...")

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",   # Replace all data
            autodetect=True
        )
    )

    job.result()  # Menunggu hingga job selesai

    print("SUCCESS — API updated to BigQuery!")


# --- 8. Run program ---
main()
