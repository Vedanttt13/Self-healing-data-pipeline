import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

DEAD    = "dead"
HEALED  = "healed"
CORRECT = "correct"


class HealData:

    def __init__(self, df: DataFrame):
        """
        Initialize HealData with the input DataFrame.
        Adds _status and _heal_log columns if not already present.

        Args:
            df: Raw input PySpark DataFrame
        """
        self.df = self._init_status_columns(df)
        logger.info("HealData initialized with status columns")

    # -----------------------------------------------------------------------
    # Private Helpers
    # -----------------------------------------------------------------------

    def _init_status_columns(self, df: DataFrame) -> DataFrame:
        """
        Add _status and _heal_log columns to DataFrame if not present.
        Called once during __init__.
        """
        if "_status" not in df.columns:
            df = df.withColumn("_status", F.lit(CORRECT))
        if "_heal_log" not in df.columns:
            df = df.withColumn("_heal_log", F.lit(None).cast(StringType()))
        return df

    def _update_status(self, condition, new_status: str, log_msg: str) -> None:
        """
        Update _status and _heal_log on self.df for rows matching condition.

        Priority rule: dead > healed > correct
          - new_status = dead   → always overwrite regardless of current status
          - new_status = healed → only overwrite if current status is correct

        Args:
            condition : PySpark column boolean condition
            new_status: "dead" or "healed"
            log_msg   : message to write into _heal_log
        """
        if new_status == DEAD:
            self.df = self.df.withColumn(
                "_status",
                F.when(condition, F.lit(DEAD)).otherwise(F.col("_status"))
            )
            self.df = self.df.withColumn(
                "_heal_log",
                F.when(condition, F.lit(log_msg)).otherwise(F.col("_heal_log"))
            )

        elif new_status == HEALED:
            self.df = self.df.withColumn(
                "_status",
                F.when(
                    condition & (F.col("_status") == CORRECT), F.lit(HEALED)
                ).otherwise(F.col("_status"))
            )
            self.df = self.df.withColumn(
                "_heal_log",
                F.when(
                    condition & (F.col("_status") == HEALED), F.lit(log_msg)
                ).otherwise(F.col("_heal_log"))
            )

    # -----------------------------------------------------------------------
    # Public Rule Functions
    # -----------------------------------------------------------------------

    def handle_null(self, rule: dict) -> None:
        """
        Rule type: not_null
        Checks if a field contains null values.

        heal.enabled = false → mark record as dead
        heal.enabled = true  → fill with fill_value from heal config, mark healed

        Args:
            rule: individual rule dict from rules.json
        """
        rule_id  = rule["id"]
        rule_name = rule["name"]
        field    = rule["field"]
        heal     = rule.get("heal", {})
        heal_on  = heal.get("enabled", False)

        logger.info(f"[{rule_id}] Applying not_null check on field: '{field}'")

        # Only act on null rows that are not already dead
        null_condition = (
            F.col(field).isNull() & (F.col("_status") != DEAD)
        )

        if not heal_on:
            log_msg = (
                f"{rule_id}|{rule_name}: field '{field}' is null, cannot heal"
            )
            self._update_status(null_condition, DEAD, log_msg)
            logger.info(f"[{rule_id}] Null rows on '{field}' marked as dead")

        else:
            fill_value = heal.get("fill_value", None)

            if fill_value is not None:
                self.df = self.df.withColumn(
                    field,
                    F.when(null_condition, F.lit(fill_value))
                     .otherwise(F.col(field))
                )

            log_msg = (
                f"{rule_id}|{rule_name}: field '{field}' was null, "
                f"filled with '{fill_value}'"
            )
            self._update_status(null_condition, HEALED, log_msg)
            logger.info(
                f"[{rule_id}] Null rows on '{field}' healed with value '{fill_value}'"
            )

    def handle_deduplication(self, rule: dict) -> None:
        """
        Rule type: deduplicate (via heal.strategy)
        Identifies duplicate rows by dedup_key field.

        Kept occurrence (first or last) → marked healed if it was in a dup group
        Non-kept duplicates             → marked dead

        Args:
            rule: individual rule dict from rules.json
        """
        rule_id   = rule["id"]
        rule_name = rule["name"]
        heal      = rule.get("heal", {})
        dedup_key = heal.get("dedup_key", rule.get("field"))
        keep      = heal.get("keep", "last")

        logger.info(
            f"[{rule_id}] Applying deduplication on key: '{dedup_key}', "
            f"keeping: '{keep}'"
        )

        # Assign row numbers within each duplicate group
        if keep == "last":
            order_col = F.desc(F.monotonically_increasing_id())
        else:
            order_col = F.asc(F.monotonically_increasing_id())

        window = Window.partitionBy(dedup_key).orderBy(order_col)
        self.df = self.df.withColumn("_row_num", F.row_number().over(window))

        # Rows with row_num > 1 are duplicates to discard
        dup_condition = (
            (F.col("_row_num") > 1) & (F.col("_status") != DEAD)
        )

        dead_log = (
            f"{rule_id}|{rule_name}: duplicate on key '{dedup_key}', "
            f"keeping '{keep}' occurrence"
        )
        self._update_status(dup_condition, DEAD, dead_log)

        # Find keys that had duplicates, mark the kept row as healed
        dup_keys = [
            row[0] for row in
            self.df.groupBy(dedup_key)
                   .agg(F.count("*").alias("_cnt"))
                   .filter(F.col("_cnt") > 1)
                   .select(dedup_key)
                   .collect()
        ]

        if dup_keys:
            kept_condition = (
                (F.col("_row_num") == 1)
                & F.col(dedup_key).isin(dup_keys)
                & (F.col("_status") == CORRECT)
            )
            healed_log = (
                f"{rule_id}|{rule_name}: kept as deduplicated record "
                f"on key '{dedup_key}'"
            )
            self._update_status(kept_condition, HEALED, healed_log)

        self.df = self.df.drop("_row_num")
        logger.info(f"[{rule_id}] Deduplication completed on key '{dedup_key}'")

    def handle_timestamp(self, rule: dict) -> None:
        """
        Rule type: date_range
        Checks if a timestamp field contains a future date.

        heal.enabled = false → mark record as dead
        heal.enabled = true  → replace with current timestamp, mark healed

        Args:
            rule: individual rule dict from rules.json
        """
        rule_id   = rule["id"]
        rule_name = rule["name"]
        field     = rule["field"]
        heal      = rule.get("heal", {})
        heal_on   = heal.get("enabled", False)
        strategy  = heal.get("strategy", "replace_with_current_timestamp")

        logger.info(
            f"[{rule_id}] Applying date_range check on field: '{field}'"
        )

        now     = datetime.now(timezone.utc)
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        future_condition = (
            F.col(field).cast("timestamp") > F.lit(now_str).cast("timestamp")
        ) & (F.col("_status") != DEAD)

        if not heal_on:
            log_msg = (
                f"{rule_id}|{rule_name}: field '{field}' is a future "
                f"timestamp, cannot heal"
            )
            self._update_status(future_condition, DEAD, log_msg)
            logger.info(
                f"[{rule_id}] Future timestamp rows on '{field}' marked as dead"
            )

        else:
            if strategy == "replace_with_current_timestamp":
                self.df = self.df.withColumn(
                    field,
                    F.when(future_condition, F.current_timestamp())
                     .otherwise(F.col(field))
                )
                log_msg = (
                    f"{rule_id}|{rule_name}: field '{field}' was future "
                    f"timestamp, replaced with current timestamp"
                )
                self._update_status(future_condition, HEALED, log_msg)
                logger.info(
                    f"[{rule_id}] Future timestamp rows on '{field}' "
                    f"healed with current timestamp"
                )
            else:
                logger.warning(
                    f"[{rule_id}] Unknown strategy '{strategy}', marking as dead"
                )
                log_msg = (
                    f"{rule_id}|{rule_name}: field '{field}' is future "
                    f"timestamp, unknown strategy '{strategy}'"
                )
                self._update_status(future_condition, DEAD, log_msg)

    def handle_regex(self, rule: dict) -> None:
        """
        Rule type: regex
        Validates a field value against a regex pattern.

        heal.enabled = false                    → mark record as dead
        heal.enabled = true + nullify_field     → set field to null, mark healed

        Args:
            rule: individual rule dict from rules.json
        """
        rule_id   = rule["id"]
        rule_name = rule["name"]
        field     = rule["field"]
        pattern   = rule["pattern"]
        heal      = rule.get("heal", {})
        heal_on   = heal.get("enabled", False)
        strategy  = heal.get("strategy", "nullify_field")

        logger.info(
            f"[{rule_id}] Applying regex validation on field: '{field}' "
            f"with pattern: '{pattern}'"
        )

        invalid_condition = (
            F.col(field).isNotNull()
            & ~F.col(field).rlike(pattern)
            & (F.col("_status") != DEAD)
        )

        if not heal_on:
            log_msg = (
                f"{rule_id}|{rule_name}: field '{field}' failed regex, "
                f"cannot heal"
            )
            self._update_status(invalid_condition, DEAD, log_msg)
            logger.info(
                f"[{rule_id}] Regex-failed rows on '{field}' marked as dead"
            )

        else:
            if strategy == "nullify_field":
                self.df = self.df.withColumn(
                    field,
                    F.when(
                        invalid_condition,
                        F.lit(None).cast(StringType())
                    ).otherwise(F.col(field))
                )
                log_msg = (
                    f"{rule_id}|{rule_name}: field '{field}' failed regex, "
                    f"value nullified"
                )
                self._update_status(invalid_condition, HEALED, log_msg)
                logger.info(
                    f"[{rule_id}] Regex-failed rows on '{field}' "
                    f"healed by nullifying field"
                )
            else:
                logger.warning(
                    f"[{rule_id}] Unknown strategy '{strategy}', marking as dead"
                )
                log_msg = (
                    f"{rule_id}|{rule_name}: field '{field}' failed regex, "
                    f"unknown strategy '{strategy}'"
                )
                self._update_status(invalid_condition, DEAD, log_msg)

    def handle_range(self, rule: dict) -> None:
        """
        Rule type: range
        Validates numeric fields against min/max bounds.

        heal.enabled = false → mark row as dead
        heal.enabled = true  → clamp values within range and mark healed

        Args:
            rule: individual rule dict from rules.json
        """

        rule_id = rule["id"]
        rule_name = rule["name"]

        field = rule["field"]
        min_val = rule["min"]
        max_val = rule["max"]

        heal = rule.get("heal", {})
        heal_on = heal.get("enabled", False)

        logger.info(
            f"[{rule_id}] Applying range validation "
            f"on field '{field}' "
            f"between {min_val} and {max_val}"
        )

        invalid_condition = (
            (
                (F.col(field) < min_val)
                | (F.col(field) > max_val)
                | (F.col(field).isNull())
            )
            & (F.col("_status") != DEAD)
        )

        if not heal_on:

            log_msg = (
                f"{rule_id}|{rule_name}: "
                f"field '{field}' outside allowed range "
                f"[{min_val}, {max_val}]"
            )

            self._update_status(
                invalid_condition,
                DEAD,
                log_msg
            )

            logger.info(
                f"[{rule_id}] Range failures "
                f"on '{field}' marked dead"
            )

        else:

            strategy = heal.get("strategy", "clamp")

            if strategy == "clamp":

                clamp_min = heal.get(
                    "clamp_min",
                    min_val
                )

                clamp_max = heal.get(
                    "clamp_max",
                    max_val
                )

                self.df = self.df.withColumn(
                    field,

                    F.when(
                        F.col(field) < clamp_min,
                        F.lit(clamp_min)
                    )

                    .when(
                        F.col(field) > clamp_max,
                        F.lit(clamp_max)
                    )

                    .when(
                        F.col(field).isNull(),
                        F.lit(clamp_min)
                    )

                    .otherwise(
                        F.col(field)
                    )
                )

                log_msg = (
                    f"{rule_id}|{rule_name}: "
                    f"field '{field}' clamped "
                    f"to range [{clamp_min}, {clamp_max}]"
                )

                self._update_status(
                    invalid_condition,
                    HEALED,
                    log_msg
                )

                logger.info(
                    f"[{rule_id}] Range failures "
                    f"on '{field}' healed via clamp"
                )

            else:

                logger.warning(
                    f"[{rule_id}] Unknown strategy "
                    f"'{strategy}'"
                )

                log_msg = (
                    f"{rule_id}|{rule_name}: "
                    f"unknown healing strategy '{strategy}'"
                )

                self._update_status(
                    invalid_condition,
                    DEAD,
                    log_msg
                )

    def get_dataframe(self) -> DataFrame:
        """Return the current state of the DataFrame after all rules applied."""
        return self.df