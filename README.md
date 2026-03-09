# Bank Market Capitalization ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline that extracts data about the world's top 10 largest banks by market capitalization, transforms currency values, and loads the results into multiple formats.

> **Note:** This project is part of my Data Engineering career development journey.

## 📋 Project Overview

This project demonstrates core data engineering skills by building an automated ETL pipeline that:

- **Extracts** bank market capitalization data from Wikipedia
- **Transforms** USD values to multiple currencies (GBP, EUR, INR)
- **Loads** processed data to both CSV and SQLite database
- **Logs** all pipeline operations with timestamps

##  Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    EXTRACT      │────▶│   TRANSFORM     │────▶│      LOAD       │
│                 │     │                 │     │                 │
│  Wikipedia API  │     │  Currency       │     │  CSV File       │
│  Web Scraping   │     │  Conversion     │     │  SQLite DB      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │    LOGGING      │
                                               │                 │
                                               │  code_log.txt   │
                                               └─────────────────┘
```

##  Project Structure

```
DataEngineeringProject/
├── banks_project.py       # Main ETL pipeline script
├── exchange_rate.csv      # Exchange rate data (USD to GBP/EUR/INR)
├── Largest_banks_data.csv # Output: Processed bank data
├── Banks.db               # Output: SQLite database
├── code_log.txt           # Output: Execution logs
├── venv/                  # Python virtual environment
└── README.md              # Project documentation
```

##  Technologies Used

- **Python 3.x** - Core programming language
- **pandas** - Data manipulation and analysis
- **BeautifulSoup4** - Web scraping
- **requests** - HTTP requests
- **SQLite3** - Database storage
- **datetime** - Timestamp logging

##  Installation

1. **Clone the repository**
```bash
git clone https://github.com/hgorkemy/DataEngineeringProject.git
cd DataEngineeringProject
```

2. **Create and activate virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows
```

3. **Install dependencies**
```bash
pip install requests beautifulsoup4 pandas
```

##  Usage

Run the ETL pipeline:
```bash
source venv/bin/activate
python banks_project.py
```

##  Pipeline Functions

| Function | Description |
|----------|-------------|
| `log_progress(message)` | Logs execution progress with timestamp to `code_log.txt` |
| `extract(url, table_attribs)` | Scrapes bank data from Wikipedia |
| `transform(df, csv_path)` | Converts market cap from USD to GBP, EUR, INR |
| `load_to_csv(df, output_path)` | Exports DataFrame to CSV file |
| `load_to_db(df, sql_connection, table_name)` | Loads DataFrame to SQLite database |
| `run_query(query_statement, sql_connection)` | Executes and displays SQL query results |

##  Output Schema

| Column | Description |
|--------|-------------|
| `Name` | Bank name |
| `MC_USD_Billion` | Market capitalization in USD (Billions) |
| `MC_GBP_Billion` | Market capitalization in GBP (Billions) |
| `MC_EUR_Billion` | Market capitalization in EUR (Billions) |
| `MC_INR_Billion` | Market capitalization in INR (Billions) |

##  Sample Output

```
                                      Name  MC_USD_Billion  MC_GBP_Billion  MC_EUR_Billion  MC_INR_Billion
0                           JPMorgan Chase          432.92          346.34          402.62        35910.71
1                          Bank of America          231.52          185.22          215.31        19204.58
2  Industrial and Commercial Bank of China          194.56          155.65          180.94        16138.75
3               Agricultural Bank of China          160.68          128.54          149.43        13328.41
4                                HDFC Bank          157.91          126.33          146.86        13098.63
5                              Wells Fargo          155.87          124.70          144.96        12929.42
6                        HSBC Holdings PLC          148.90          119.12          138.48        12351.26
7                           Morgan Stanley          140.83          112.66          130.97        11681.85
8                  China Construction Bank          139.82          111.86          130.03        11598.07
9                            Bank of China          136.81          109.45          127.23        11348.39
```

##  Log Output

```
2026-Feb-04-16:45:21 : Preliminaries complete. Initiating ETL process
2026-Feb-04-16:45:23 : Data extraction complete. Initiating Transformation process
2026-Feb-04-16:45:24 : Data transformation complete. Initiating Loading process
2026-Feb-04-16:45:24 : Data saved to CSV file
2026-Feb-04-16:45:24 : SQL Connection initiated
2026-Feb-04-16:45:24 : Data loaded to Database as a table, Executing queries
2026-Feb-04-16:45:24 : Process Complete
2026-Feb-04-16:45:24 : Server Connection closed
```

##  SQL Queries Executed

1. **Select all records:**
```sql
SELECT * FROM Largest_banks
```

2. **Average market cap in GBP:**
```sql
SELECT AVG(MC_GBP_Billion) FROM Largest_banks
```

3. **Top 5 bank names:**
```sql
SELECT Name FROM Largest_banks LIMIT 5
```

##  Data Sources

- **Bank Data:** [Wikipedia - List of largest banks](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)
- **Exchange Rates:** IBM Skills Network dataset

##  Skills Demonstrated

- Web Scraping
- Data Extraction & Transformation
- Database Operations (SQL)
- File I/O Operations
- Logging & Monitoring
- Python Best Practices
- ETL Pipeline Design

##  Author

**Halil Görkem Yiğit**

- GitHub: [@hgorkemy](https://github.com/hgorkemy)

## 📄 License

This project is open source and available under the [MIT License](LICENSE).
