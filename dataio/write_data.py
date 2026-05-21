import json
import logging

from pyspark.sql import SparkSession

from read_data import DataReader
# from pipeline import pipeline
# from write_data import write_data

# Configure Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def load_json_config(file_path: str) -> dict:
    """
    Load JSON configuration file
    """
    try:
        logger.info(f"Loading config file: {file_path}")

        with open(file_path, "r") as file:
            config = json.load(file)

        logger.info(f"Successfully loaded: {file_path}")

        return config

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format in: {file_path}")
        raise

    except Exception as e:
        logger.error(f"Error loading config file: {str(e)}")
        raise


def create_spark_session():
    """
    Create Spark Session
    """
    try:
        logger.info("Creating Spark Session")

        spark = (
            SparkSession.builder
            .appName("SelfHealingDataPipeline")
            .getOrCreate()
        )
        logger.info("Spark Session created successfully")
        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark Session: {str(e)}")
        raise


def pipeline(pipeline_dict, input_data, spark):
    """
    Main pipeline function

    Parameters:
    ----------
    pipeline_dict : dict
        Rules configuration

    input_data : DataFrame
        Source dataframe

    spark : SparkSession
        Active spark session
    """

    try:
        logger.info("Running pipeline transformations")

        # ------------------------------
        # Add transformation logic here
        # ------------------------------

        transformed_df = input_data

        logger.info("Pipeline execution completed")

        return transformed_df

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise


def write_data(source_conf, output_df):
    """
    Placeholder write function
    """
    try:
        logger.info("Writing output data")

        # --------------------------------
        # Add write logic later
        # --------------------------------

        logger.info("Data write completed")

    except Exception as e:
        logger.error(f"Data write failed: {str(e)}")
        raise


def elt(source_dict, rules_dict, spark):
    """
    ELT Wrapper Function

    Flow:
    -----
    1. Read Data
    2. Run Pipeline
    3. Write Data
    """

    try:

        logger.info("Starting ELT Process")

        # Get source config
        source_conf = source_dict["source"]

        # Read Input Data
        reader = DataReader(source_conf, spark)

        input_data = reader.data_read()

        logger.info("Input data loaded successfully")

        # Run Pipeline
        output_df = pipeline(
            pipeline_dict=rules_dict,
            input_data=input_data,
            spark=spark
        )

        logger.info("Pipeline processing completed")

        # Write Output Data
        write_data(source_conf, output_df)

        logger.info("ELT Process Completed Successfully")

    except Exception as e:
        logger.error(f"ELT Process Failed: {str(e)}")
        raise


if __name__ == "__main__":

    try:

        logger.info("Starting Wrapper")

        # Create Spark Session
        spark = create_spark_session()

        # Load Source Config
        source_dict = load_json_config("source.json")

        # Load Rules Config
        rules_dict = load_json_config("rules.json")

        logger.info("Configurations loaded successfully")

        # Run ELT
        elt(
            source_dict=source_dict,
            rules_dict=rules_dict,
            spark=spark
        )

        logger.info("Wrapper Execution Completed")

    except Exception as e:

        logger.error(f"Wrapper Execution Failed: {str(e)}")