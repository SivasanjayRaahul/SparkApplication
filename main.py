import datetime
import sys

from lib import utils, data_loader, transform_data, data_writer

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark application {local, prod} {date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    if job_run_env != "LOCAL" and job_run_env != "PROD":
        print("Invalid env: PROD or LOCAL ")
        sys.exit(-1)

    date_env = sys.argv[2]
    date_format = "%Y-%m-%d"
    date = datetime.datetime.strptime(date_env, date_format)

    print("Initializing Spark Job in " + job_run_env)
    spark = utils.get_spark_session(job_run_env)

    print("Loading Data " + job_run_env)
    df = data_loader.load(job_run_env, spark, date)

    print("Transforming data " + job_run_env)
    transform_df = transform_data.to(df)

    print("Writing Data " + job_run_env)
    data_writer.write(job_run_env, transform_df, date)

    print("Completed")
