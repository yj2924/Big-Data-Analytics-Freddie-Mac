"""Small helpers shared by all Member-A jobs (local mode and Dataproc)."""
import os
from pyspark.sql import SparkSession, functions as F


def get_spark(app_name: str) -> SparkSession:
    """Local runs pick up SPARK_MASTER / SPARK_DRIVER_MEMORY; on Dataproc the cluster config wins."""
    b = SparkSession.builder.appName(app_name)
    if os.environ.get("SPARK_MASTER"):                       # e.g. local[*] when running on a laptop
        b = (b.master(os.environ["SPARK_MASTER"])
              .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
              .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64")))
    b = b.config("spark.sql.session.timeZone", "UTC").config("spark.sql.parquet.compression.codec", "snappy")
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def na_to_null(col, *na_codes):
    """Freddie Mac encodes 'not available' with sentinel codes (9999, 999, 99, 9). Turn them into NULL."""
    c = F.trim(col)
    cond = (c == "") | c.isNull()
    for code in na_codes:
        cond = cond | (c == str(code))
    return F.when(cond, F.lit(None)).otherwise(c)


def yyyymm_to_date(col):
    """'200603' -> date 2006-03-01"""
    return F.to_date(F.concat(F.trim(col), F.lit("01")), "yyyyMMdd")


def vintage_from_loan_id(col):
    """Loan sequence numbers look like F06Q10000059: F + YY + Qn + serial -> origination year / quarter.
    Freddie Mac loan IDs start in 1999, so YY = 99 -> 1999, everything else -> 20YY."""
    yy = F.substring(col, 2, 2).cast("int")
    year = F.when(yy == 99, F.lit(1999)).otherwise(yy + 2000)
    quarter = F.substring(col, 5, 1).cast("int")
    return year, quarter
