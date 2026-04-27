# Xetra ETL Pipeline Project

## Overview

This project is an end-to-end ETL pipeline built using Python and Docker. It extracts stock market data, transforms it into analytical format, and stores final outputs in AWS S3.

## Features

* Automated ETL process
* Dockerized deployment
* AWS S3 integration
* Config driven execution
* Unit testing implemented
* Production ready structure

## Tech Stack

* Python
* Pandas
* AWS S3
* Docker
* Pipenv
* PyYAML
* unittest

## Project Structure

* configs/
* tests/
* Xetra/common/
* transformers/
* run.py

## How to Run

```bash
docker build -t xetra-etl .
docker run xetra-etl
```

## Output

* Parquet reports stored in S3
* Meta files generated
* Logs available

## Author

Kanishka Bharara
