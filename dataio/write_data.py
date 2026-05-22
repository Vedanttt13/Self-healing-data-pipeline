import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class DataWriter:

    def __init__(self, config: dict, spark):
        """
        config -> complete source.json dictionary
        """
        self.config = config
        self.spark = spark
        self.targets = config.get("target", [])

    def data_write(self, df: DataFrame, data_type: str):
        """
        Write dataframe to configured targets

        Parameters:
        ----------
        df : Spark DataFrame
        data_type : str
            correct / healed / dead
        """

        try:
            if df is None:
                logger.warning(f"{data_type} dataframe is None")
                return

            logger.info(f"Writing {data_type} dataframe")

            for target in self.targets:

                if not target.get("enabled", True):
                    continue

                target_type = target["type"]

                # Write to local storage
                if target_type == "local_storage":

                    path = (
                        f"{target['connection']['path']}/"
                        f"{data_type}"
                    )

                    file_format = target["connection"].get(
                        "format",
                        "parquet"
                    )

                    mode = target["load"].get(
                        "mode",
                        "append"
                    )

                    logger.info(f"Writing to: {path}")

                    (
                        df.write
                        .mode(mode)
                        .format(file_format)
                        .save(path)
                    )

                    logger.info(
                        f"{data_type} written successfully"
                    )

                # Placeholder for BigQuery
                elif target_type == "bigquery":

                    logger.info(
                        "BigQuery write not implemented yet"
                    )

                else:
                    logger.warning(
                        f"Unsupported target type: "
                        f"{target_type}"
                    )

        except Exception as e:
            logger.error(
                f"Failed writing {data_type}: {e}"
            )
            raise