# Self-Healing Data Pipeline 🚀

A PySpark-based configurable pipeline that validates incoming records, automatically fixes recoverable issues, and routes data into separate outputs:

- Clean records
- Healed records
- Rejected records

---

## Architecture

```text
                Source Data
                     |
                     v
             +----------------+
             |   Data Reader  |
             +----------------+
                     |
                     v
          +----------------------+
          | Validation + Healing |
          +----------------------+
                     |
      -----------------------------------
      |                |                |
      v                v                v

   Clean Data      Healed Data     Rejected Data
```

---

## Project Structure

```text
project/

├── bin/
│   └── wrapper.py
│
├── config/
│   ├── test_rules.json
│   └── test_source.json
│
├── dataio/
│   ├── read_data.py
│   └── write_data.py
│
├── heal/
│   ├── code_master.py
│   └── heal_data.py
│
└── README.md
```

---

## Features

Supported validations:

- Not Null
- Range Validation
- Regex Validation
- Duplicate Detection
- Date Validation

Supported healing:

- Deduplicate records
- Clamp invalid values
- Nullify invalid fields
- Replace timestamps

---

## Config Driven

Pipeline behavior is controlled using:

```text
test_rules.json
test_source.json
```

No code changes required.

---

## Tech Stack

- Python
- PySpark
- Spark SQL
- JSON Configs
- Logging

---

## Run Project

Install dependencies:

```bash
pip install pyspark
```

Run:

```bash
python bin/wrapper.py
```

---

## Goal

✔ Detect bad data

✔ Heal recoverable records

✔ Separate rejected records

✔ Improve data quality automatically

---

## License

MIT