# Databricks notebook source
# Python base packages
import json
import os
from datetime import datetime, timedelta
# Dependent Python packages (included in poetry):
import pandas as pd
import numpy as np
import urllib3
from dotenv import load_dotenv
# from pandas.io.json import json_normalize
urllib3.disable_warnings()


def get_articles_from_file():
    load_dotenv()
    
    # Load news dataset from file:
    path_csv = "\\\\PASv00127/ZF_ONELOGIC_EXCHANGE\Market Analytics/python_scripts_input/" # os.getenv('STORAGE_LOCATION')
    filename_news = "eventRegistry_news.csv" # os.getenv('FILENMAE_COMPANY_NEWS') # "eventRegistry_news.csv"
    file_location_load = path_csv + filename_news
    dataset = pd.read_csv(file_location_load, na_values="nan", keep_default_na=False)
    return dataset

def get_new_articles_from_eventRegistry():
    load_dotenv()
    
    # Load dataset: Company Names
    path_csv = os.getenv('STORAGE_LOCATION') # "\\\\PASv00127/ZF_ONELOGIC_EXCHANGE\Market Analytics/python_scripts_input/"
    filename_company_names = os.getenv('FILENAME_COMPANY_NAMES') # "Market_Analytics_Company_Names_List_Prepared.csv"
    file_location_load_1 = path_csv + filename_company_names
    dataset_1 = pd.read_csv(file_location_load_1, na_values="nan", keep_default_na=False)
    
    # Load dataset: Blacklist
    path_csv = os.getenv('STORAGE_LOCATION') # "\\\\PASv00127/ZF_ONELOGIC_EXCHANGE\Market Analytics/python_scripts_input/"
    filename_blacklist = os.getenv('FILENAME_SOURCE_BLACKLIST') # "Market_Analytics_Source_Blacklist.csv"
    file_location_load_2 = path_csv + filename_blacklist
    dataset_2 = pd.read_csv(file_location_load_2, na_values="nan", keep_default_na=False)
    
    # Load API-Key
    apikey = os.getenv('API_KEY_EVENTREGISTRY') # "845d2401-2e2b-4335-988d-1ea9abfe21f5"
    
    # Create datasets
    companies = dataset_1['Company_Name'].tolist()
    news_company_name_1 = dataset_1['Alternative_Company_Name_1'].tolist()
    news_company_name_2 = dataset_1['Alternative_Company_Name_2'].tolist()
    blacklist_Source_URIs = [ str(val).strip() for val in dataset_2['Source_URI']]
    
    columns = ['Company_Name', 'Title', 'Content', 'Language', 'URL', 'Date', 'Time', 'publishedAt', 'Image', 'Sentiment', 'Shares', 'Article_Weight', 'Source', 'Source_Rank', 'Source_Location']
    df = pd.DataFrame(columns=columns)
    
    # Get news for each company:
    for num, comp in enumerate(companies):
        try:
            all_comp_names = [str(comp)]
            if news_company_name_1[num] != "nan" and len(news_company_name_1[num]) is not None:
              all_comp_names = all_comp_names + [str(news_company_name_1[num])]
            if news_company_name_2[num] is not None:
              all_comp_names = all_comp_names + [str(news_company_name_2[num])]
            
            num_days = 30
            sort_type = 'date' # 'rel'
            # apikey = '845d2401-2e2b-4335-988d-1ea9abfe21f5'
            start = (datetime.now() - timedelta(days=num_days)).strftime('%Y-%m-%d')
            end = datetime.now().strftime('%Y-%m-%d')
            
            link = "http://eventregistry.org/api/v1/article/getArticles"
            
            body = json.dumps(
            {
                "apiKey": apikey,
                "action": "getArticles",
                "articlesPage": 1,
                "articlesCount": 100,
                "articlesSortBy": sort_type,
                "articleBodyLen": -1,
                "query": {
                    "$query": {
                        "$or": [
                            {
                                "dateStart": start, 
                                "dateEnd": end, 
                                "lang": { "$or": ["eng", "deu"]}, 
                                "keywordLoc": "body,title", 
                                "keyword": { "$or": all_comp_names }, 
                                "ignoreSourceUri": { "$or": blacklist_Source_URIs }
                            }
                        ],
                        "$not": {
                            "dateStart": start, 
                            "dateEnd": end,  
                            "keywordLoc": "body,title", 
                            "keyword": { "$or": all_comp_names }, 
                            "sourceUri": { "$or": blacklist_Source_URIs }
                        }
                    },
                    "$filter": {
                        "dataType": ["news", "pr"],  
                        "isDuplicate": "keepAll", 
                        "hasDuplicate": "keepAll", 
                        "hasEvent": "keepAll", 
                        "startSourceRankPercentile": 0, 
                        "endSourceRankPercentile": 100
                    }
                },
                "includeArticleTitle": True,
                "includeArticleBasicInfo": True,
                "includeArticleBody": True,
                "includeArticleSocialScore": True,
                "includeArticleSentiment": True,
                "includeArticleConcepts": False,
                "includeArticleCategories": True,
                "includeArticleLocation": True,
                "includeArticleImage": True,
                "includeArticleDuplicateList": True,
                "includeArticleOriginalArticle": True,
                "includeSourceTitle": True,
                "includeSourceDescription": True,
                "includeSourceLocation": True,
                "includeSourceRanking": True,
                "includeConceptLabel": False
            }
            )
        
            print("Company-Name: " + str(comp))
            # print("Number-Days: " + str(num_days))
            # print("Link: " + str(link))
            # print("Body: " + str(body))
            
            http = urllib3.PoolManager()
            #http = urllib3.ProxyManager("http://anonymous@zf-proxy.zf-world.com:8070", cert_reqs='CERT_NONE')
            r = http.request('GET', link, headers={'Content-Type': 'application/json'}, body=body)
            data = json.loads(r.data.decode('utf-8'))
            r.close()
                
            news = []
            for entry in data['articles']['results']:
                article = []
                article.append(comp) # Company_Name
                
                # normal columns
                table_columns = ['title', 'body', 'lang', 'url', 'date', 'time', 'dateTime', 'image', 'sentiment', 'shares', 'wgt']
                for col in table_columns:
                    try:
                        article.append(str(entry[col]))
                    except:
                        article.append('')
                
                # source columns:
                try:
                    article.append(str(entry['source']['title']))
                except:
                    article.append('')
                
                try:
                    article.append(str(entry['source']['ranking']['importanceRank']))
                except:
                    article.append('')
                
                try:
                    article.append(str(entry['source']['location']['country']['label']['eng']))
                except:
                    article.append('')
                
                # append the article to all news
                news.append(article)
        
        except:
            news = []
            print("Error occured! Could not get news for Company: " + str(comp))
    
        # ignore empty result
        if news != []:
            print("Number of news for Company " + str(comp) + ": " + str(len(news)))
            new_df = pd.DataFrame(data=np.array(news), columns=columns)
            df = df.append(new_df, ignore_index=True)
    
    # COMMAND ----------
    
    cols = list(df)
    df[cols] = df[cols].replace({'\n':' ', ',': '', ';':'' }, regex=True)
    df = df.drop(['Content'], axis = 1)
    
    # Save results to file:
    path_csv = os.getenv('STORAGE_LOCATION')
    filename_news = os.getenv('FILENMAE_COMPANY_NEWS') # "eventRegistry_news.csv"
    file_location_save = path_csv + filename_news
    df.to_csv(file_location_save, index=False)


# spark_df=spark.createDataFrame(df)

# save_location= "abfss://rootaap@azweaappaapdatalake.dfs.core.windows.net/DIVI/apidata/"
# csv_location = save_location+"temp.folder"
# file_location = save_location+"eventRegistry_News.csv"

#spark_df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").save(file_location)
# spark_df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header","true").save(csv_location)

# file = dbutils.fs.ls(csv_location)[-1].path
# dbutils.fs.cp(file, file_location)
# dbutils.fs.rm(csv_location, recurse=True)
