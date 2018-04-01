import pandas as pd
import numpy as np
import json
import requests


#products_services = ['checking_accounts', 'credit_cards', 'investing', 'loans', 'wires']
products_services = ['loans']

key = 'XYZXYZXYZXYZXYZXYZ'
cx = 'XYZXYZXYZXYZXYZXYZXYZ'
number_of_batches = 5
number_of_queries_per_batch = 1


url = "https://www.googleapis.com/customsearch/v1"
parameters = {"q": "best low interest credit card",
              "cx": cx,
              "key": key,
              }

def process_search(results):
    link_list = [item["link"] for item in results["items"]]
    df = pd.DataFrame(link_list, columns=["link"])
    df["title"] = [item["title"] for item in results["items"]]
    df["snippet"] = [item["snippet"] for item in results["items"]]
    df["searchTerms"] = [results["queries"]["request"][0]["searchTerms"] for item in results["items"]]
    df["rank"] = [x for x in range(1, len(results["items"])+1) if item in results["items"]]
    return df

def get_search_results(url, parameters):     
    page = requests.request("GET", url, params=parameters)
    results = json.loads(page.text)
    return process_search(results)



def record_search_results(search_df, file_name):
    with open(file_name, 'a') as f:
        parameters = {"q": search_df['Keyword'],
                    "cx": cx,
                    "key": key,
                    }
        try:
            search_results = get_search_results(url, parameters)
        except:
            raise Exception('Failed to perform search query at index' + search_df.index.values)
        search_results.to_csv(f, header=False, encoding = 'utf-8')
        #return search_df

def record_all_search_results(product_service_df, product_service):
    file_name = "search_results.csv"
    batch = 0
    while batch < number_of_batches:
        batch = batch + 1
        #start_idx, end_idx = get_next_search_index_range(product_service_df, number_of_queries_per_batch)
        list_idx = get_next_search_index_range(product_service_df, number_of_queries_per_batch)
        try:
            product_service_df.loc[list_idx].apply(lambda x: record_search_results(x, file_name), axis = 1)
            #update the document containing search queries
            update_executed_search_queries(product_service_df, product_service, list_idx)
            
        except Exception as inst:
            print (inst.args)

def get_next_search_index_range(product_service_df, number_of_queries_per_batch):
    #if search column is not in the dataframe, create one and fill it with nan's
    if('searched' not in product_service_df):
        product_service_df['searched'] = np.nan * len(product_service_df)
        #start_idx = 0
        #end_idx = number_of_queries_per_batch
        list_idx = range(10)
    else:
        #start_idx = int(product_service_df['searched'].last_valid_index()) + 1
        #end_idx = start_idx + number_of_queries_per_batch
        list_idx = product_service_df[product_service_df['searched'].isnull()].index.tolist()[0:10]
        
    #return start_idx, end_idx
    return list_idx
    
def update_executed_search_queries(product_service_df, product_service , list_idx):
    #product_service_df['searched'].loc[list_idx] = 1
    #product_service_df['searched'].set_value(list_idx, 1, 1)
    product_service_df.ix[list_idx,'searched'] = 1
    print(product_service_df.ix[list_idx,'Keyword'])
    
    product_service_df[['Ad group', 'Keyword', 'Currency', 'Avg. Monthly Searches (exact match only)', 'Competition', 'Suggested bid', 'Impr. share', 'Organic impr. share', 'Organic avg. position', 'In account?', 'In plan?', 'Extracted From', 'searched']].to_csv('data/'+product_service+'.csv', sep='\t', encoding='utf-16')
    
def read_data(products_services):
    for product_service in products_services:
        product_service_df = pd.read_csv('data/'+product_service+'.csv', sep='\t', encoding='utf-16')
        product_service_df = product_service_df.drop_duplicates('Keyword') #drop duplicate keywords
        #sort by traffic in descending order
        product_service_df = product_service_df.sort_values('Avg. Monthly Searches (exact match only)', ascending = False, na_position = 'last')
        record_all_search_results(product_service_df, product_service)

read_data(products_services)