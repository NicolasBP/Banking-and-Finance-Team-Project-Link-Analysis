import pandas as pd
import numpy as np


search_results = pd.read_csv('search_results.csv', sep=',', header = None, names = ['Index','URL', 'Title', 'Description', 'Keyword', 'Rank'], usecols = ['Index', 'URL', 'Title', 'Description', 'Keyword', 'Rank'])
search_data = pd.read_csv('data/loans.csv', sep='\t', encoding='utf-16', usecols = ['Avg. Monthly Searches (exact match only)', 'Keyword'])


expected_traffic_proportion = {'1':0.5636,
                               '2':0.1345,
                               '3':0.0982,
                               '4':0.04,
                               '5':0.0473,
                               '6':0.0327,
                               '7':0.036,
                               '8':0.0291,
                               '9':0.0145,
                               '10':0.0255}

competitor_url = {'citi.' : 'Citigroup',
		  'citigroup' : 'Citigroup',
		  'citimortgage': 'Citigroup',
                  'bankofamerica' : 'Bank of America',
                  'wellsfargo' : 'Wells Fargo',
                  'citizensbank' : 'Citizens Bank', 
                  'hsbc' : 'HSBC', 
                  'chase' : 'JP Morgan Chase', 
                  'santanderbank' : 'Banco Santander',
                  'lendingclub' : 'Lending Club'
                 }
                 
competitors = list(competitor_url.keys())

def get_competitors(df):
    if any(competitor in df['URL'].iloc[0] for competitor in competitors):
        return df

def estimate_traffic(row):
    return round(row['Avg. Monthly Searches (exact match only)'] * expected_traffic_proportion[str(row['Rank'])])

def get_competitor(row):
    for key in competitor_url:
        if key in row['URL']:
            return competitor_url[key]


search_results_competitors = search_results[search_results['URL'].str.contains('|'.join(competitors), regex = True)]
search_results_competitors = pd.merge(search_results_competitors, search_data, how = 'left', on = 'Keyword')
search_results_competitors['Expected Searches'] = 0 * len(search_results_competitors)
search_results_competitors['Expected Searches'] = search_results_competitors.apply(estimate_traffic, axis = 1)
search_results_competitors['Competitor'] = search_results_competitors.apply(get_competitor, axis = 1)


competitor_estimated_traffic = pd.pivot_table(search_results_competitors, index = ['Competitor'], values = 'Expected Searches', aggfunc = np.sum)

link_estimated_traffic = pd.pivot_table(search_results_competitors, index = ['URL', 'Keyword'], values = 'Expected Searches', aggfunc = np.sum)

#search_results_summary = search_results_competitors.groupby(by = ['URL', 'Keyword'], sort = False).apply(get_competitors).reset_index(drop=True)['Rank'].mean()
#search_results_summary = [search_result_summary for search_result_summary in search_results_summary if not np.isnan(search_result_summary['URL'].iloc[0])]
competitor_estimated_traffic.to_csv('competitor_estimated_traffic.csv')
link_estimated_traffic.to_csv('link_estimated_traffic.csv')

#print(search_results_summary)