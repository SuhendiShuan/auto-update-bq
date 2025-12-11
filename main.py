import requests
import pandas as pd
from google.cloud import bigquery

def main():
    url = "https://43.218.20.129:447/dev/get_cds.php?p_cek=x&p_cds=ZA_HENDITESTV&p_user=AO000018&p_pass=@1Nusantara&p_filter=tahun;eq;%272025%27;and;periode;eq;%2712%27;"
    response = requests.get(url, verify=False)
    data = response.json()
    df = pd.DataFrame(data)

    client = bigquery.Client()
    table_ref = "load-api-update.load_api_update.za_hendi_test"

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()
    print("SUCCESS — API updated to BigQuery!")

if __name__ == "__main__":
    main()
