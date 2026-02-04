# Code for ETL operations on Bank Market Cap data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime

# Constants - Bilinen değerleri tanımlama
URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
EXCHANGE_RATE_CSV = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
TABLE_ATTRIBS_EXTRACT = ['Name', 'MC_USD_Billion']
TABLE_ATTRIBS_FINAL = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
OUTPUT_CSV_PATH = './Largest_banks_data.csv'
DB_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'
LOG_FILE = 'code_log.txt'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing.

    Bu fonksiyon, kod yürütmenin belirli bir aşamasındaki mesajı
    bir log dosyasına kaydeder. Fonksiyon hiçbir şey döndürmez.

    Format: <time_stamp> : <message>
    '''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Örnek: 2024-Jan-15-14:30:45
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(LOG_FILE, 'a') as f:
        f.write(f'{timestamp} : {message}\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.
    '''

    # Web sayfasına istek gönder
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # "By market capitalization" (Piyasa Değeri) başlığı altındaki tabloyu bul
    # Tablo, sayfadaki ilk wikitable'dır
    tables = soup.find_all('table', {'class': 'wikitable'})

    # İlk tablodan (piyasa değeri tablosu) verileri çıkar
    data = []

    # İlk wikitable piyasa değeri verilerini içerir
    table = tables[0]
    rows = table.find_all('tr')

    for row in rows[1:]:  # Başlık satırını atla
        cols = row.find_all('td')
        if len(cols) >= 3:
            # Banka adı ikinci sütunda (index 1), anchor etiketi içinde
            name_col = cols[1]
            # strip=True ile '\n' gibi karakterler kaldırılır
            name = name_col.get_text(strip=True)

            # Piyasa değeri üçüncü sütunda (index 2)
            mc_text = cols[2].get_text(strip=True)
            try:
                # Değeri float formatına dönüştür
                mc_value = float(mc_text)
                data.append({table_attribs[0]: name, table_attribs[1]: mc_value})
            except ValueError:
                continue

    # Toplanan verilerden dataframe oluştur
    df = pd.DataFrame(data, columns=table_attribs)

    # En büyük 10 bankayı al
    df = df.head(10)

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies.

    
    '''

    # Döviz kuru CSV dosyasını oku
    exchange_df = pd.read_csv(csv_path)

    # Döviz kurları için bir sözlük oluştur
    # İlk sütun (Currency) -> anahtar, İkinci sütun (Rate) -> değer
    exchange_rate = exchange_df.set_index('Currency')['Rate'].to_dict()

    # MC_GBP_Billion sütununu ekle (USD * GBP kuru, 2 ondalık basamağa yuvarlanmış)
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rate['GBP'], 2)

    # MC_EUR_Billion sütununu ekle (USD * EUR kuru, 2 ondalık basamağa yuvarlanmış)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rate['EUR'], 2)

    # MC_INR_Billion sütununu ekle (USD * INR kuru, 2 ondalık basamağa yuvarlanmış)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rate['INR'], 2)

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.
    '''
    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.
    '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing.

    The query statement is printed along with the query output.
    '''
    print(f'\nQuery: {query_statement}')
    print('Result:')
    result = pd.read_sql(query_statement, sql_connection)
    print(result)
    print()


# Main execution
if __name__ == '__main__':

    # ========== TASK 1: Define known values ==========
    # Constants are defined above, make first log entry
    log_progress('Preliminaries complete. Initiating ETL process')

    # ========== TASK 2: Call extract() function ==========
    # Extract table information under 'By market capitalization' heading
    # from the given URL and save it to a dataframe
    print('=' * 70)
    print('TASK 2: Data Extraction (Extract)')
    print('=' * 70)

    df = extract(URL, TABLE_ATTRIBS_EXTRACT)

    print('Extracted Data:')
    print(df)
    print()

    log_progress('Data extraction complete. Initiating Transformation process')

    # ========== TASK 3: Call transform() function ==========
    # Add columns for Market Cap in GBP, EUR and INR
    # based on exchange rate information (rounded to 2 decimal places)
    print('=' * 70)
    print('TASK 3: Data Transformation (Transform)')
    print('=' * 70)

    df = transform(df, EXCHANGE_RATE_CSV)

    print('Transformed Data:')
    print(df)
    print()

    # Check the 5th row of MC_EUR_Billion column
    print(f"MC_EUR_Billion column 5th row: {df['MC_EUR_Billion'][4]}")
    print()

    log_progress('Data transformation complete. Initiating Loading process')

    # ========== TASK 4: Call load_to_csv() ==========
    # Load transformed dataframe to an output CSV file
    print('=' * 70)
    print('TASK 4: Load to CSV File')
    print('=' * 70)

    load_to_csv(df, OUTPUT_CSV_PATH)

    print(f'Data saved to {OUTPUT_CSV_PATH}')
    print()

    log_progress('Data saved to CSV file')

    # ========== TASK 5: Call load_to_db() ==========
    # Load transformed dataframe to a SQL database server as a table
    print('=' * 70)
    print('TASK 5: Load to Database')
    print('=' * 70)

    # Initialize SQLite3 connection
    conn = sqlite3.connect(DB_NAME)
    log_progress('SQL Connection initiated')
    print(f'SQLite3 database connection initiated: {DB_NAME}')

    # Load data to database
    load_to_db(df, conn, TABLE_NAME)

    print(f'Data loaded to {DB_NAME} database, table: {TABLE_NAME}')
    print()

    log_progress('Data loaded to Database as a table, Executing queries')

    # ========== TASK 6: Call run_query() ==========
    # Run queries on the database table
    print('=' * 70)
    print('TASK 6: Run SQL Queries')
    print('=' * 70)

    # Query 1: Print the contents of the entire table
    query1 = f'SELECT * FROM {TABLE_NAME}'
    run_query(query1, conn)

    # Query 2: Print the average market capitalization of all banks in Billion GBP
    query2 = f'SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}'
    run_query(query2, conn)

    # Query 3: Print only the names of the top 5 banks
    query3 = f'SELECT Name FROM {TABLE_NAME} LIMIT 5'
    run_query(query3, conn)

    log_progress('Process Complete')

    # Close SQLite3 connection
    conn.close()
    log_progress('Server Connection closed')

    print('Database connection closed')
    print()

    # ========== TASK 7: Verify log file contents ==========
    print('=' * 70)
    print('TASK 7: Log File Contents (code_log.txt)')
    print('=' * 70)
    with open(LOG_FILE, 'r') as f:
        print(f.read())
