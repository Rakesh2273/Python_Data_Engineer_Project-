from io import StringIO
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
from rich import print  # Alternative to icecream for debugging


def record_log(entry)
    Logs each process step with a timestamp.
    with open('process_log.txt', 'a') as log_file
        log_file.write(f'{datetime.now()} - {entry}n')


def fetch_data(source_url, identifier)
    Extracts table data from the given URL and returns a DataFrame.
    response = requests.get(source_url)
    html_soup = BeautifulSoup(response.text, 'html.parser')
    target_table = html_soup.find('span', string=identifier).find_next('table')
    dataframe = pd.read_html(StringIO(str(target_table)))[0]
    
    record_log('Data successfully retrieved. Proceeding to transformation.')
    return dataframe


def process_data(dataframe, exchange_file)
    Applies currency conversions based on exchange rates from CSV.
    conversion_rates = pd.read_csv(exchange_file, index_col=0).to_dict()['Rate']
    
    dataframe['GBP_Value'] = round(dataframe['Market cap (US$ billion)']  conversion_rates['GBP'], 2)
    dataframe['EUR_Value'] = round(dataframe['Market cap (US$ billion)']  conversion_rates['EUR'], 2)
    dataframe['INR_Value'] = round(dataframe['Market cap (US$ billion)']  conversion_rates['INR'], 2)
    
    print([bold green]Sample EUR Value[bold green], dataframe['EUR_Value'][4])
    
    record_log('Transformation step completed. Proceeding to storage.')
    return dataframe


def save_as_csv(dataframe, file_path)
    Exports the processed data to a CSV file.
    dataframe.to_csv(file_path, index=False)
    record_log('Data successfully saved as CSV.')


def save_to_database(dataframe, db_conn, table)
    Stores the DataFrame into an SQL database.
    dataframe.to_sql(table, db_conn, if_exists='replace', index=False)
    record_log('Database storage completed.')


def execute_query(statement, db_conn)
    Runs an SQL query and returns the result set.
    with db_conn.cursor() as cursor
        cursor.execute(statement)
        output = cursor.fetchall()
    record_log('Query executed successfully.')
    return output


if __name__ == '__main__'
    source_url = 'httpsweb.archive.orgweb20230908091635httpsen.wikipedia.orgwikiList_of_largest_banks'
    csv_output_path = 'Processed_Banks_Data.csv'
    db_name = 'FinancialData.db'
    table_name = 'Global_Banks'
    
    record_log('ETL process initiated.')
    
    bank_data = fetch_data(source_url, 'By market capitalization')
    processed_data = process_data(bank_data, 'exchange_rate.csv')
    
    save_as_csv(processed_data, csv_output_path)
    
    with sqlite3.connect(db_name) as connection
        save_to_database(processed_data, connection, table_name)
        
        print([bold cyan]Top 5 Banks[bold cyan], execute_query('SELECT Bank name FROM Global_Banks LIMIT 5', connection))
        print([bold cyan]Average Market Cap in GBP[bold cyan], execute_query('SELECT AVG(GBP_Value) FROM Global_Banks', connection))
