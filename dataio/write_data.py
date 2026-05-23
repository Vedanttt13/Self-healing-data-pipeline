import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class DataWriter:

    def __init__(self, config: dict, spark):
        self.config = config
        self.spark = spark
        self.targets = config.get("target", [])

    def _set_spark_config(self, source_type: str, source_conf: dict):
        try:
            if source_type == "s3":
                conn = source_conf["connection"]
                self.spark.conf.set("spark.hadoop.fs.s3a.access.key", conn["access_key"])
                self.spark.conf.set("spark.hadoop.fs.s3a.secret.key", conn["secret_key"])
                self.spark.conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

        except Exception as e:
            logger.error(f"Spark config failed: {e}")
            raise

    def write_local_storage(self, df: DataFrame, target: dict, target_id: str):
        path = f"{target['connection']['path']}/{target_id}"
        fmt = target["load"].get("format", "parquet")

        df.write.mode("append").format(fmt).save(path)
        logger.info(f"Written to local storage: {path}")

    def write_s3(self, df: DataFrame, target: dict, target_id: str):
        conn = target["connection"]
        path = f"s3a://{conn['bucket']}/{conn['folder']}/{target_id}"
        fmt = target["load"].get("format", "parquet")

        df.write.mode("append").format(fmt).save(path)
        logger.info(f"Written to S3: {path}")

    def write_delta(self, df: DataFrame, target: dict):
        path = target["connection"]["path"]

        df.write.format("delta").mode("append").save(path)
        logger.info(f"Written to Delta: {path}")

    def data_write(self, df: DataFrame, target_id: str):
        try:
            if df is None:
                logger.warning(f"{target_id} dataframe is None")
                return

            target = next(
                (t for t in self.targets if t.get("id") == target_id),
                None
            )

            if target is None:
                raise ValueError(f"No target found with id: '{target_id}'")

            if not target.get("enabled", True):
                logger.info(f"Target '{target_id}' is disabled, skipping.")
                return

            target_type = target["type"]
            self._set_spark_config(source_type=target_type, source_conf=target)

            if target_type == "local_storage":
                self.write_local_storage(df, target, target_id)
            elif target_type == "s3":
                self.write_s3(df, target, target_id)
            elif target_type == "delta":
                self.write_delta(df, target)

        except Exception as e:
            logger.error(f"Write failed for target '{target_id}': {e}")
            raise