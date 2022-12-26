# Ecommerce Data Engineering (Brazilian E-Commerce Platform Olyst)

Inserting raw data to Bigquey  , Creating Models for Anayltical Purpose



## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the requriedd libraries.

```bash
pip install google-cloud
pip install pandas
```


## Setting up BigQuery authentication

```python
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'./bq_credential.json'
```
or 

Please place your credentials file, named "bq_credential.json", in the main directory.

## Data Source
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Raw Data
Raw data must be in folder "Data" and in csv format
