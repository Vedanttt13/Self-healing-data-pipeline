import logging

# Configure Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


class DataReader:
    def __init__(self, source: dict, spark):
        """
        Initialize DataReader

        Parameters:
        ----------
        source : dict
            Source configuration dictionary

        spark : SparkSession
            Active Spark session
        """

        self.source = source
        self.spark = spark

    def data_read(self):
        """
        Main method to read data from different sources
        """

        try:
            source_type = self.source["type"]

            logger.info(f"Reading data from source type: {source_type}")

            if source_type in ["postgres", "mysql", "oracle", "sqlserver"]:
                return self.read_jdbc()

            elif source_type == "warehouse":
                return self.read_warehouse()

            elif source_type == "object_storage":
                return self.read_object_storage()

            else:
                raise ValueError(
                    f"Unsupported source type: {source_type}"
                )

        except KeyError as e:
            logger.error(
                f"Missing required source configuration key: {e}"
            )
            raise

        except Exception as e:
            logger.error(f"Error while reading data: {str(e)}")
            raise

    def read_jdbc(self):
        """
        Read data from JDBC sources

        Examples:
        - PostgreSQL
        - MySQL
        - SQL Server
        """

        try:
            connection = self.source["connection"]

            jdbc_url = connection["url"]
            username = connection["username"]
            password = connection["password"]

            schema = self.source["schema"]
            table = self.source["table"]

            full_table_name = f"{schema}.{table}"

            logger.info(
                f"Reading JDBC table: {full_table_name}"
            )

            df = (
                self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", full_table_name)
                .option("user", username)
                .option("password", password)
                .option("driver", "org.postgresql.Driver")
                .load()
            )

            logger.info("JDBC data loaded successfully")

            return df

        except KeyError as e:
            logger.error(
                f"Missing JDBC configuration key: {e}"
            )
            raise

        except Exception as e:
            logger.error(
                f"Failed to read JDBC source: {str(e)}"
            )
            raise

    def read_warehouse(self):
        """
        Read data from warehouse

        Examples:
        - Snowflake
        - Redshift
        - BigQuery
        """

        try:
            warehouse_config = self.source["connection"]

            table = warehouse_config["table"]

            logger.info(
                f"Reading warehouse table: {table}"
            )

            df = (
                self.spark.read.format("parquet").load(
                    f"/warehouse/{table}"
                )
            )

            logger.info(
                "Warehouse data loaded successfully"
            )

            return df

        except KeyError as e:
            logger.error(
                f"Missing warehouse configuration key: {e}"
            )
            raise

        except Exception as e:
            logger.error(
                f"Failed to read warehouse source: {str(e)}"
            )
            raise

    def read_object_storage(self):
        """
        Read data from object storage

        Examples:
        - AWS S3
        - Azure Blob Storage
        - Google Cloud Storage
        """

        try:
            storage_config = self.source["connection"]

            path = storage_config["path"]
            file_format = storage_config.get(
                "format", "parquet"
            )

            logger.info(
                f"Reading object storage path: {path}"
            )

            df = (
                self.spark.read.format(file_format).load(
                    path
                )
            )

            logger.info(
                "Object storage data loaded successfully"
            )

            return df

        except KeyError as e:
            logger.error(
                f"Missing object storage configuration key: {e}"
            )
            raise

        except Exception as e:
            logger.error(
                f"Failed to read object storage source: {str(e)}"
            )
            raise