#
# ETL Project

This project implements an ETL (Extract, Transform, Load) pipeline that extracts data from a JSON file, transforms it, and loads it into a PostgreSQL database.

## Project Overview

The ETL pipeline consists of three main stages:
- **Extraction**: Reads raw candidate data from a JSON file.
- **Transformation**: Processes and cleans the data, preparing it for loading into the database.
- **Loading**: Inserts the processed data into the appropriate tables in a PostgreSQL database.

## Project Structure

```plaintext
ETL_Project/
│
├── data/                       # Directory for data files (input/output)
│   ├── input/                  # Raw data files
│   └── output/                 # Transformed data files
│
├── src/                        # Source code for the ETL process
│   ├── extraction.py           # Script for data extraction
│   ├── transformation.py       # Script for data transformation
│   ├── loading.py              # Script for loading data into PostgreSQL
│   └── utils.py                # Utility functions
│
├── config/                     # Configuration files
│   └── config.yaml             # Configuration file for database and file paths
├── scripts/                    # Execution scripts
│   └── run_etl.py              # Main script to run the ETL pipeline
├── .gitignore                  # Git ignore file
├── README.md                   # Project documentation
└── requirements.txt            # Python dependencies
