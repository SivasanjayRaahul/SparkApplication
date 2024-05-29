from pyspark.sql import DataFrame


def write(env: str, df: DataFrame, date):
    if env == "PROD":
        prefix = f"s3://videodatacount/timestamp={date}/"
        df.write.csv(prefix)
