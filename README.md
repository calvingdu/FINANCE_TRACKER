# Transactions Tracker
[Transctions Tracker] - Data Pipeline for Canadian Bank Transactions
By Calvin Du

## Description
This project is a data pipeline designed to handle transaction data from various Canadian banks. It extracts, transforms, and loads the data into a MongoDB database, providing a centralized repository for reporting and expense tracking purposes.

# Usage
## Copy code
git clone https://github.com/calvingdu/Transactions-Pipeline.git

## Install the required dependencies:
poetry install
pip3 install -e '.[dev]'

## Configure the necessary environment variable
export PYTHONPATH=.

export PYTHON_ENV=develop

## Prefect
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect server start
