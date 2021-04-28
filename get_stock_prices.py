# Databricks notebook source
# Python base packages
import json
import time
import os
# Dependent Python packages (included in poetry):
import pandas as pd
import numpy as np
import urllib3
import logging
from dotenv import load_dotenv
urllib3.disable_warnings()


def get_stock_prices_from_file():
    load_dotenv()
    
    # Load stock prices dataset from file:
    path_csv = os.getenv('STORAGE_LOCATION')
    filename_stock_quotes = os.getenv('FILENAME_STOCK_QUOTES') # "stock_quotes.csv"
    file_location_load = path_csv + filename_stock_quotes
    dataset = pd.read_csv(file_location_load, delimiter=";", decimal=',', na_values="nan", keep_default_na=False)
    return dataset

def get_new_stock_prices_from_api():
    load_dotenv()
    
    # Load dataset: Stock symbols
    path_csv = os.getenv('STORAGE_LOCATION') # "\\\\PASv00127/ZF_ONELOGIC_EXCHANGE\Market Analytics/python_scripts_input/"
    filename_stock_symbols = os.getenv('FILENAME_STOCK_SYMBOLS') # "stock_symbols.csv"
    file_location_load = path_csv + filename_stock_symbols
    dataset = pd.read_csv(file_location_load, delimiter=",", na_values="nan", keep_default_na=False)
    
    # Load API-Key
    apikey = os.getenv('API_KEY_ALPHAVANTAGE') # "YDC3NLI9NL0V8GH6"
    
    # Initialize logger for logging:
    if "logger" not in globals():
        path_for_logger = os.getenv('STORAGE_LOCATION')
        logFormatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        logger = logging.getLogger()
        logfileHandler = logging.FileHandler(path_for_logger+"processing.log", 
                                             mode="a")  # write messages to the logfile
        logfileHandler.setFormatter(logFormatter)
        logfileHandler.setLevel(logging.INFO)
        logconsoleHandler = logging.StreamHandler()  # write messages to the console
        logconsoleHandler.setFormatter(logFormatter)
        logconsoleHandler.setLevel(logging.INFO)
        logger.addHandler(logfileHandler)
        logger.addHandler(logconsoleHandler)
        logger.setLevel(logging.INFO)
    else:
        print("Logger already initialized.")
    
    # Create datasets
    companies = dataset['Company_Name'].tolist()
    ids = dataset['Company_ID'].tolist()
    regions = dataset['Stock_Region'].tolist()
    currencies = dataset['Stock_Currency'].tolist()
    symbols = dataset['Stock_Symbol'].tolist()
    columns = ['Company_Name', 'Company_ID', 'Stock_Symbol', 'Date', 'Stock_price', 'Volume', 'Stock_Region', 'Stock_Currency']
    df = pd.DataFrame(columns=columns)
    
    logger.info("Starting to collect all stock prices.")
    
    # Get stock prices for each stock quote:
    for comp, symbol, id, region, curr in zip(companies, symbols, ids, regions, currencies):
        try:
            if (symbol != None and symbol != '' and symbol != 'N/A'):
                outputsize = "compact"  # "full"
                # apikey = "YDC3NLI9NL0V8GH6"
                function = "TIME_SERIES_DAILY_ADJUSTED"
                link = "https://www.alphavantage.co/query?function={}&symbol={}&outputsize={}&datatype=json&apikey={}"
                link = link.format(function, symbol, outputsize, apikey)
                print("API Call for Company: "+ comp +" ("+ link +")")
                # http = urllib3.PoolManager()
                # http = urllib3.ProxyManager("http://anonymous@zf-proxy.zf-world.com:8070", cert_reqs='CERT_NONE')
                http = urllib3.ProxyManager("http://ep.threatpulse.net:80", cert_reqs='CERT_NONE')
                #r = http.request('GET', link, headers={'Content-Type': 'application/json'})
                r = http.request('GET', link)
                data = json.loads(r.data.decode('utf-8'))
                r.close()
                if(data != None):
                    quotes = []
                    try:
                        for elem in data['Time Series (Daily)']:
                            quote = []
                            quote.append(comp)
                            quote.append(id)
                            quote.append(symbol)
                            quote.append(elem)
                            quote.append(str(data['Time Series (Daily)'][elem]['5. adjusted close']))
                            quote.append(str(data['Time Series (Daily)'][elem]['6. volume']))
                            quote.append(region)
                            quote.append(curr)
                            quotes.append(quote)
                    except:
                        logger.error("Error occured while trying to get quote data for the Company: " + str(comp) + " - with the Symbol: " + str(symbol))
    
                    if quotes != []:
                        new_df = pd.DataFrame(data=np.array(quotes), columns=columns)
                        df = df.append(new_df, ignore_index=True)
                    else:
                        logger.info("Could not get any stock prices for the Company: " + str(comp) + " - with the Symbol: " + str(symbol))
    
                    # AlphaVantage is limited to 5 API requests per minute (one each 12 Seconds)
                    time.sleep(12)   # Pauses the program for 12 seconds 
        
        except:
            logger.error("Error occured! Could not get stock prices for the Company: " + str(comp) + " - with the Symbol: " + str(symbol))
    
    logger.info("Finished collecting all stock prices.")
    
    # Convert datatypes
    df["Stock_price"] = pd.to_numeric(df["Stock_price"])
    df["Volume"] = pd.to_numeric(df["Volume"])
    # Make negative stock prices positve:
    df["Stock_price"] = df["Stock_price"].abs()
    
    # Save results to file:
    path_csv = os.getenv('STORAGE_LOCATION')
    filename_stock_quotes = os.getenv('FILENAME_STOCK_QUOTES') # "stock_quotes.csv"
    file_location_save = path_csv + filename_stock_quotes
    df.to_csv(file_location_save, index=False, header=True, decimal=',', sep=';', float_format='%.2f')


# save_location= "abfss://rootaap@azweaappaapdatalake.dfs.core.windows.net/DIVI/apidata/"
# csv_location = save_location+"temp.folder"
# file_location = save_location+"stock_symbols_multi_test_result_fin.csv"

# dbutils.fs.rm(file_location, recurse=True)

# COMMAND ----------

# save_location= "abfss://rootaap@azweaappaapdatalake.dfs.core.windows.net/DIVI/apidata/"
# csv_location = save_location+"temp.folder"
# file_location = save_location+"stock_symbols_multi_test_result_fin.csv"

# spark_df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").save(csv_location)

# file = dbutils.fs.ls(csv_location)[-1].path
# dbutils.fs.cp(file, file_location)
# dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------


