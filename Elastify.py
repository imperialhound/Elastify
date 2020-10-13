from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.helpers import scan as escan
import pandas as pd
import json 
import collections

class Elastify:
    '''
    Used to easily convert ElasticSearch json data to Pandas dataframe and Vice Versa.
    
    Attributes:
    
    file_path: Path of CSV or Excel file to imported into ElasticSearch Node, default None
    Host: ElasticSearch Node location, default localhost:9200
    index_name: Desired name of index to push to pull data from 
    es: Elastic Search Configuration and mananager 
    '''
    
    def __init__(self, file_path=None, HOST="http://localhost:9200", index_name='my_index'):
        self.file_path = file_path
        self.HOST = HOST
        self.index_name = index_name
        self.es = es = Elasticsearch(HOST)
    
    # Importer of Excel or CSV file and create pandas dataframe
    def dataframe_former(self):
        # Create dataframe from csv or excel 
        if self.file_path.endswith('.csv'):
            df = pd.read_csv(self.file_path)
    
        elif self.file_path.endswith('.xlsm'):
            df = pd.read_excel(self.file_path)
    
        else:
            print('File is not CSV or Excel')
        
        return df
     
    # Filter document into seperate features
    def filterKeys(self, document):
            df = self.dataframe_former()
            features = df.columns.to_list()
            return {key: document[key] for key in features }
        
    # Iterate over each document and create dictionary for each document foucsing on key features
    def to_dict(self):
        df = self.dataframe_former()
        
        # Iterate through rows and convert into Dict 
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                    "_index": self.index_name,
                    "_type": "_doc",
                    "_id" : f"{index}",
                    "_source": self.filterKeys(document),
                
                }
    
    # Upload dictionaries as JSON to ElasticSearch node in bulk
    def to_es(self):
          helpers.bulk(self.es, self.to_dict())

    
    # Pull ElasticSearch index from node and generate pandas dataframe
    def to_df(self):
        
        body = {
          "size": 1000,
          "query": {
          "match_all": {}
              }
            }
        
        response = escan(client=self.es,
                 index=self.index_name,
                 query=body, request_timeout=30, size=1000)

        # Initialize a double ended queue
        output_all = collections.deque()
        
        # Extend deque with iterator
        output_all.extend(response)
        
        # Convert deque to DataFrame
        output_df = pd.json_normalize(output_all)
    
        return output_df



    
