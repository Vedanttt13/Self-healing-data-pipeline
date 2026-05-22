import json
import logging

from pyspark.sql import SparkSession
from dataio import DataReader
from dataio import DataWriter
from heal import CodeMaster

# from pipeline import pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def load_json_config(file_path: str) -> dict:
    """Load JSON configuration"""

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
        logger.error(f"Error loading config file: {e}")
        raise


def create_spark_session():
    """Create Spark Session"""

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
        logger.error(f"Failed to create Spark Session: {e}")
        raise


def run_pipeline(rules_dict, input_data, spark):
    """Apply transformations using CodeMaster"""
 
    try:
        logger.info("Running pipeline transformations")
 
        master = CodeMaster(
            rules_dict=rules_dict,
            input_data=input_data,
            spark=spark
        )
 
        correct_df, healed_df, dead_df = master.run()
 
        logger.info("Pipeline execution completed")
        return correct_df, healed_df, dead_df
 
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise

def elt(source_dict, rules_dict, spark):
    """Main ELT workflow"""

    try:
        logger.info("Starting ELT Process")

        source_conf = source_dict["source"]

        reader = DataReader(source_conf, spark)
        input_data = reader.data_read()

        logger.info("Input data loaded successfully")

        correct_df, healed_df, dead_df = run_pipeline(
            rules_dict=rules_dict,
            input_data=input_data,
            spark=spark
        )

        logger.info("Pipeline processing completed")

        # CHANGE HERE
        writer = DataWriter(source_dict, spark)

        writer.data_write(correct_df, "correct")
        writer.data_write(healed_df, "healed")
        writer.data_write(dead_df, "dead")

        logger.info("ELT Process Completed Successfully")

    except Exception as e:
        logger.error(f"ELT Process Failed: {e}")
        raise

if __name__ == "__main__":

    spark = None

    try:
        logger.info("Starting Wrapper")

        spark = create_spark_session()

        source_dict = load_json_config("source.json")
        rules_dict = load_json_config("rules.json")

        logger.info("Configurations loaded successfully")

        elt(
            source_dict=source_dict,
            rules_dict=rules_dict,
            spark=spark
        )

        logger.info("Wrapper Execution Completed")

    except Exception as e:
        logger.error(f"Wrapper Execution Failed: {e}")

    finally:
        if spark:
            spark.stop()
            logger.info("Spark Session stopped")