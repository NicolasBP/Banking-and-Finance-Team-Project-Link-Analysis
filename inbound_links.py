from urlparse import urlparse
import pandas as pd
import numpy as np
import json
import requests

number_of_batches = 1


'''def get_inbound_links(dataframe):
    batch = 0
    while batch < number_of_batches:
        batch = batch + 1
        try:
            dataframe['Inbound Links'] = dataframe.apply(lambda x: record_search_results(x), axis = 1)
            #update the document containing search queries
            
        except Exception as inst:
            print (inst.args)
        return dataframe
'''



key = 'XYZXYZXYZXYZXYZXYZ'
cx = 'XYZXYZXYZXYZXYZXYZ'
number_of_batches = 1
number_of_queries_per_batch = 1


url = "https://www.googleapis.com/customsearch/v1"
parameters = {"q": "best low interest credit card",
              "cx": cx,
              "key": key,
              }


def find_root_domain(url):
    parsed_uri = urlparse(url)
    #domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    domain = '{uri.netloc}'.format(uri=parsed_uri)
    domain = domain.replace('www.','')
    return domain

def process_search(results):
    num_links = results['searchInformation']['totalResults']
    return num_links

def get_search_results(url, parameters):     
    page = requests.request("GET", url, params=parameters)
    results = json.loads(page.text)
    return process_search(results)



def record_search_results(search_df):
    parameters = {"q": search_df['Keyword'],
                "cx": cx,
                "key": key,
                }
    try:
        search_df['Inbound Links'] = get_search_results(url, parameters)
    except:
        raise Exception('Failed to perform search query at index' + search_df.index.values)
    return search_df
    

def update_executed_search_queries(inbound_links_df, tracking_file_name , list_idx):
    inbound_links_df.ix[list_idx,'searched'] = 1
    print(inbound_links_df.ix[list_idx,'URL'])
    
    inbound_links_df.to_csv(tracking_file_name + '.csv', sep=',', encoding='utf-8')


def get_next_search_index_range(inbound_links_df, number_of_queries_per_batch):
    #if search column is not in the dataframe, create one and fill it with nan's
    if('searched' not in inbound_links_df):
        inbound_links_df['searched'] = np.nan * len(inbound_links_df)
        list_idx = range(1)
    else:
        list_idx = inbound_links_df[inbound_links_df['searched'].isnull()].index.tolist()[0:10]
        
    #return list_idx
    return list_idx


def record_all_search_results(inbound_links_df, tracking_file_name):
    #file_name = "inbound_links.csv"
    batch = 0
    while batch < number_of_batches:
        batch = batch + 1
        list_idx = get_next_search_index_range(inbound_links_df, number_of_queries_per_batch)
        try:
            inbound_links_df.loc[list_idx] = inbound_links_df.loc[list_idx].apply(lambda x: record_search_results(x), axis = 1)
            update_executed_search_queries(inbound_links_df, tracking_file_name , list_idx)
        except Exception as inst:
            print (inst.args)
        return inbound_links_df

def get_links(links_new, links_current, links_file_name):
    #links_new = pd.read_csv('link_estimated_traffic.csv', sep=',', header = None, names = ['URL'], usecols = ['URL'])
    #links_current = pd.read_csv('inbound_links.csv', usecols=['URL', 'Root Domain', 'Keyword', 'searched', 'Inbound Links'])
    links = pd.concat([links_current, links_new])
    #links = links_current.append(links_new, ignore_index = True)
    links = links.drop_duplicates('URL', keep = 'first', inplace = False)
    links.reset_index(inplace = True)
    links['Root Domain'] = links.apply(lambda x: find_root_domain(x['URL']), axis = 1)
    links['Keyword'] = "-site:" + links['Root Domain'] + " " + "link:" + links['URL']
    
    links.to_csv(links_file_name + '.csv')
    #with open(links_file_name + '.csv', 'a') as f:
    #    links.to_csv(f, header=False)
    return links


links = pd.read_csv('inbound_links.csv', usecols=['URL', 'Root Domain', 'Keyword', 'searched', 'Inbound Links'])
links_new = pd.read_csv('link_estimated_traffic.csv', sep=',', names = ['URL'], usecols = ['URL'])
links_new['Root Domain'] = np.nan * len(links_new)
links_new['Keyword'] = np.nan * len(links_new)
links_new['searched'] = np.nan * len(links_new)
links_new['Inbound Links'] = np.nan * len(links_new)
urls = get_links(links_new, links, 'inbound_links')


'''urls = links.drop_duplicates('URL', inplace = False) #drop duplicate URLs
urls['Root Domain'] = urls.apply(lambda x: find_root_domain(x['URL']), axis = 1)
urls['Keyword'] = "-site:" + urls['Root Domain'] + " " + "link:" + urls['URL']'''

urls = record_all_search_results(urls, 'inbound_links')
