\# Healthcare Claims ETL Pipeline



\## Overview

An end-to-end ETL pipeline that processes raw healthcare 

transaction data and loads it into a structured SQL Server 

database for analytics.



\## Data Sources

\- \*\*HL7 ADT\*\* — Patient admission and demographic data

\- \*\*EDI 837\*\* — Insurance claims submitted by hospitals

\- \*\*EDI 835\*\* — Payment remittance from insurance companies



\## ETL Pipeline Flow

```

Raw Healthcare Files (HL7, EDI 837, EDI 835)

&#x20;           ↓

Python Parsing Scripts (Extract)

&#x20;           ↓

Data Cleaning \& Validation (Transform)

&#x20;           ↓

SQL Server Database (Load)

&#x20;           ↓

Analytics SQL Queries

```



\## Database Tables

\- \*\*patients\*\* — Patient demographic data from HL7 ADT

\- \*\*claims\*\* — Insurance claims from EDI 837

\- \*\*payments\*\* — Payment responses from EDI 835



\## Technologies Used

\- Python 3.14 (pandas, pyodbc)

\- SQL Server Express

\- Healthcare EDI formats (HL7 ADT, EDI 837, EDI 835)

\- Git \& GitHub



\## Project Structure

```

healthcare-claims-pipeline2/

├── raw\_data/          # Source healthcare files

│   ├── adt\_patients.hl7

│   ├── claims\_837.edi

│   └── payments\_835.edi

├── scripts/           # Python ETL scripts

│   ├── parse\_hl7.py   # HL7 ADT parser

│   ├── parse\_837.py   # EDI 837 parser

│   ├── parse\_835.py   # EDI 835 parser

│   ├── transform.py   # Data cleaning \& validation

│   └── load.py        # SQL Server loader

├── sql/               # Database scripts

│   ├── schema.sql     # Table creation

│   └── analytics.sql  # Analytics queries

└── README.md

```



\## Analytics Queries

\- Total billed vs paid amount by department

\- Claim status summary (Approved/Denied/Pending)

\- Top patients by claim amount

\- Payer wise performance analysis

\- Pending claims report



\## How to Run

1\. Clone the repository

2\. Install dependencies: `pip install pandas pyodbc`

3\. Run schema.sql in SQL Server to create tables

4\. Run ETL pipeline: `python scripts/load.py`

5\. Run analytics.sql for insights



\## Author

Myakala Uday Kiran Reddy

