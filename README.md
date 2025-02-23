Overview

This ETL (Extract, Transform, Load) pipeline is designed to fetch global banking data, process it with currency conversions, and store it in a database for further analysis. The script ensures data integrity and logging throughout the process.

Features

Extract: Scrapes banking data from Wikipedia using BeautifulSoup.

Transform: Converts market capitalization from USD to GBP, EUR, and INR using exchange rates.

Load: Saves data in CSV format and loads it into an SQLite database.

Logging: Tracks each processing step in a log file for monitoring.

Prerequisites

Ensure you have the following dependencies installed:

pip install requests beautifulsoup4 pandas sqlite3 rich

Usage

Run the script using:

Code.py

Outputs

Processed_Banks_Data.csv: Transformed dataset.

FinancialData.db: SQLite database storing processed data.

process_log.txt: Log file tracking the ETL process.

SQL Queries for Analysis

Use these SQL queries to analyze stored data:

SELECT "Bank name" FROM Global_Banks LIMIT 5;
SELECT AVG(GBP_Value) FROM Global_Banks;

