import logging
from pyspark.sql import functions as F
from heal_data import HealData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class CodeMaster:

    def __init__(self, rules_dict, input_data, spark):
        self.rules = rules_dict.get("rules", [])
        self.spark = spark
        self.healer = HealData(input_data)

        self.correct_df = None
        self.healed_df = None
        self.dead_df = None


    def apply_rules(self):

        for rule in self.rules:

            rule_id = rule.get("id")
            rule_name = rule.get("name")
            rule_type = rule.get("type", "").lower()
            heal_strategy = rule.get(
                "heal", {}
            ).get("strategy", "").lower()

            logger.info(
                f"Applying rule {rule_id} - {rule_name}"
            )

            if rule_type == "not_null":
                self.healer.handle_null(rule)

            elif rule_type == "regex":
                self.healer.handle_regex(rule)

            elif rule_type == "date_range":
                self.healer.handle_timestamp(rule)

            elif rule_type == "range":
                self.healer.handle_range(rule)

            elif rule_type == "schema":
                self.healer.handle_schema(rule)

            elif heal_strategy == "deduplicate":
                self.healer.handle_deduplication(rule)

            else:
                logger.warning(
                    f"No handler found for {rule_id}"
                )


    def split_dataframe(self):

        df = self.healer.get_dataframe()

        self.correct_df = df.filter(
            F.col("_status") == "correct"
        )

        self.healed_df = df.filter(
            F.col("_status") == "healed"
        )

        self.dead_df = df.filter(
            F.col("_status") == "dead"
        )


    def run(self):

        logger.info("Pipeline started")

        self.apply_rules()
        self.split_dataframe()

        logger.info("Pipeline completed")

        return (
            self.correct_df,
            self.healed_df,
            self.dead_df
        )