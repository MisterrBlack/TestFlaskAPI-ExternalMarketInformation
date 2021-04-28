# Databricks notebook source
# Python base packages
import json
import time
import os
# Dependent Python packages (included in poetry):
import pandas as pd
import numpy as np
import urllib3
from dotenv import load_dotenv
urllib3.disable_warnings()

# COMMAND ----------

# dataset = spark.read.format("csv").options(header='true', delimiter = ';').load("/FileStore/tables/Market_Analytics_Company_Names_For_Symbols_prepared_sorted_v2.csv")
#CMP_NAME;CMP_ID

load_dotenv()
path_csv = os.getenv('STORAGE_LOCATION') # "\\\\PASv00127/ZF_ONELOGIC_EXCHANGE\Market Analytics/python_scripts_input/"
filename_company_names = os.getenv('FILENAME_COMPANY_NAMES_FOR_SYMBOLS') # "Market_Analytics_Company_Names_For_Symbols_prepared_sorted_v2.csv"
file_location_load = path_csv + filename_company_names
dataset = pd.read_csv(file_location_load, delimiter = ';', na_values="nan", keep_default_na=False)

# COMMAND ----------

# dataset=dataset.toPandas()

# COMMAND ----------

companies = dataset['Company_Name'].tolist()
ids = dataset['Company_ID'].tolist()
# ids = dataset.index + 1
columns = ['Company_Name', 'Company_ID', 'Stock_Name', 'Stock_Symbol', 'Stock_Region', 'Stock_Currency']
df = pd.DataFrame(columns=columns)

# COMMAND ----------

for comp, id in zip(companies, ids):
    try:
        if (comp != None and comp != '' and comp != 'N/A'):
            #outputsize not needed, output is alwas the same
            #outputsize = "compact"  # "full"
            apikey = "8DO5R9KI0TJ792R2"
            function = "SYMBOL_SEARCH"
            #keyword cant contain spaces
            # keyword = comp.split()
            keyword = comp.replace(" ", "%20")
            link = "https://www.alphavantage.co/query?function={}&keywords={}&datatype=json&apikey={}"
            link = link.format(function, keyword, apikey)
            print("API Call for Company: "+ comp +" ("+ link +")")
        
            http = urllib3.PoolManager()
            r = http.request('GET', link)
            data = json.loads(r.data.decode('utf-8'))
            r.close()
                
            quotes = []
            try:
                for elem in data['bestMatches']:
                    quote = []  
                    #minimum required confidence
                    if (float(elem['9. matchScore']) > 0.299):
                        quote.append(comp)
                        quote.append(id)
                        #specific values from json
                        quote.append(str(elem['2. name']))
                        quote.append(str(elem['1. symbol']))
                        quote.append(str(elem['4. region']))
                        quote.append(str(elem['8. currency']))
                        #--
                        quotes.append(quote)
                        #print(quotes)
            except:
                print("Error occured while trying to get quote data!")
             
            if (quotes != []):
                new_df = pd.DataFrame(data=np.array(quotes), columns=columns)
                df = df.append(new_df, ignore_index=True)
                
            # AlphaVantage is limited to 5 API requests per minute (one each 12 Seconds)
            time.sleep(12)   # Pauses the program for 12 seconds 
            
    except:
        print("Error occured! Could not get stock symbols for Company: " + str(comp))
        
        

# COMMAND ----------

# display(df)

# COMMAND ----------

# spark_df=spark.createDataFrame(df)

# COMMAND ----------

path_csv = os.getenv('STORAGE_LOCATION')
filename_stock_symbols = os.getenv('FILENAME_STOCK_SYMBOLS') # "stock_symbols.csv"
file_location_save = path_csv + filename_stock_symbols
df.to_csv(file_location_save, index=False)

# save_location= "/mnt/DIVI/apidata/"
# csv_location = save_location+"temp.folder"
# file_location = save_location+"stock_symbols_v2.csv"

# spark_df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").save(csv_location)

# file = dbutils.fs.ls(csv_location)[-1].path
# dbutils.fs.cp(file, file_location)
# dbutils.fs.rm(csv_location, recurse=True)
