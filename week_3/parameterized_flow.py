from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

# @task(retries = 3 ,  cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
@task(log_prints=True )
def fetch(dataset_url:str) -> pd.DataFrame:
    """Read Taxi data from web into pandas Dataframe"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # print(f"columns: {df.dtypes}")
    # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"The N Rows: {df.shape}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write Dataframe out as parquet file"""
    Path(f"data/fhv").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/fhv/{dataset_file}.csv")
    df.to_csv(path, compression="gzip")
    return path

@task(log_prints=True , timeout_seconds=300000)
def write_gcs(path: Path) -> None:
    """Upload parquet to gcs"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("yellow")
    # time out attribute worked but the file is 100 mb and it took too long to upload
    gcp_cloud_storage_bucket_block.upload_from_path(path,path,timeout = 300000)
    return


@flow(log_prints=True)
def etl_web_to_gcs(year, month) -> None:
    """ The Main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean ,dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021
):
    for month in months:
        etl_web_to_gcs(year, month)


if __name__ == "__main__":
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019

    etl_parent_flow(months, year)

        
    # etl_parent_flow.serve(name="my-first-deployment",
    #                 tags=["onboarding"],
    #                 parameters={"months": months, "color" : color , "year" : year},
    #                 interval=60)
