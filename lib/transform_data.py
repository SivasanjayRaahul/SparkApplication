from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def remove_all_null(df: DataFrame):
    return df.na.drop()


def groupby_count(df: DataFrame, column_name: str):
    return df.groupBy(column_name).count()


def to(df: DataFrame):
    df = df.transform(remove_all_null)
    df = groupby_count(df, "video_id")
    return df.select(F.col("video_id").cast("int"), F.col("count").cast("int"))
