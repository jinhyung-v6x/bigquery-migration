import os
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timedelta


def setup_environ():
    credentials_path = "./credentials/vrew_metric_accessor.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path


def generate_dates(start_str, end_str):
    # Convert the input strings to datetime objects
    start_date = datetime.strptime(start_str, "%Y%m%d")
    end_date = datetime.strptime(end_str, "%Y%m%d")

    # List comprehension to generate dates
    date_list = [
        (start_date + timedelta(days=x)).strftime("%Y%m%d")
        for x in range((end_date - start_date).days + 1)
    ]

    return date_list


def get_history_table_schema():
    schema = [
        bigquery.SchemaField("uid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("planName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("originalPrice", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("expirationDate", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("recentPaidDate", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField(
            "startDateOfCurrentSubscription", "DATETIME", mode="NULLABLE"
        ),
        bigquery.SchemaField("created", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField(
            "startDateOfFirstSubscription", "DATETIME", mode="NULLABLE"
        ),
        bigquery.SchemaField("startDateOfFirstTermpass", "DATETIME", mode="NULLABLE"),
    ]
    return schema


def get_snapshot_table_schema():
    schema = [
        bigquery.SchemaField("uid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("in", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("out", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("planName_in", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price_in", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("currency_in", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("planName_out", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price_out", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("currency_out", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("planName_curr", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price_curr", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("currency_curr", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "startDateOfCurrentSubscription", "DATETIME", mode="NULLABLE"
        ),
        bigquery.SchemaField(
            "startDateOfFirstSubscription", "DATETIME", mode="NULLABLE"
        ),
        bigquery.SchemaField("startDateOfFirstTermpass", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("recentPaidDate", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("expirationDate", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("signupDate", "DATETIME", mode="NULLABLE"),
    ]
    return schema


def import_table_from_gcs_to_bigquery(
    bucket_name, gcs_filename, dataset_id, table_id, schema
):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set up the BigQuery dataset and table
    # project_id = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]["project_id"]
    project_id = "semiotic-nexus-199404"
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Set up the GCS URI
    uri = f"gs://{bucket_name}/{gcs_filename}"

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,  # Assuming the file is a CSV
        schema=schema,
        skip_leading_rows=1,  # Skip header row
    )

    # API request - starts the query
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

    # Waits for the job to complete
    load_job.result()

    # Checks for errors
    if load_job.errors:
        print("Errors:", load_job.errors)
    else:
        print(f"Loaded data from {uri} into {dataset_id}.{table_id}.")


def import_history_table(date: str):
    bucket_name = "v6x-payment-snapshot-v2"
    gcs_filename = f"history/history_{date}.csv"
    dataset_id = "vrew_payment_history_v2_test"
    table_id = f"history_local_{date}"

    print(f"{date} start")
    import_table_from_gcs_to_bigquery(
        bucket_name,
        gcs_filename,
        dataset_id,
        table_id,
        schema=get_history_table_schema(),
    )
    print(f"✅ {date} end")


def import_snapshot_table(date: str):
    bucket_name = "v6x-payment-snapshot-v2"
    gcs_filename = f"snapshot/snapshot_{date}.csv"
    dataset_id = "vrew_payment_snapshot_v2"
    table_id = f"snapshot_release_{date}"

    print(f"{date} start")
    import_table_from_gcs_to_bigquery(
        bucket_name,
        gcs_filename,
        dataset_id,
        table_id,
        schema=get_snapshot_table_schema(),
    )
    print(f"✅ {date} end")


def main():
    setup_environ()
    start_date = "20230502"
    end_date = "20240508"

    dates = generate_dates(start_date, end_date)
    print(f"from {start_date} to {end_date}, {len(dates)} days")

    for date in dates:
        import_snapshot_table(date)


if __name__ == "__main__":
    main()
