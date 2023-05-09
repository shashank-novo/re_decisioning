# core libraries
import pickle
import os
import pandas as pd
import numpy as np
import warnings 
warnings.filterwarnings("ignore")
from datetime import datetime
from queries import final_features

os.chdir('../')
cwd = os.getcwd()
print(cwd)
 
os.chdir('./code/')
cwd = os.getcwd()
print(cwd)

from config.config import SQLQuery
querySno = SQLQuery('snowflake')


class Score: 

    def __init__(self) -> None:
      self.features = ['distinct_debit_txns_1m',
                      'median_amount_debited_2m',
                      'ratio_debit_credit_2m',
                      'ratio_ach_credit_freq_total_credit_1m',
                      'ratio_ach_debit_credit_freq_1m',
                      'median_running_balance_2nd_m',
                      'ratio_revenue_1m_2m',
                      'distinct_cashins_1m_prev',
                      'ratio_amt_drawn_mom',
                      'ratio_amt_cashin_mom']

    
    def preprocess(self, df, feature_set, data_params, transformer):
        temp = df.copy()

        # treat missing values
        temp[feature_set] = temp[feature_set].fillna(data_params.set_index('feature').to_dict()['median'])
        
        # min max capping
        lower_limit = data_params.set_index('feature').loc[feature_set, 'lower_limit']
        upper_limit = data_params.set_index('feature').loc[feature_set, 'upper_limit']
        temp[feature_set] = temp[feature_set].clip(lower_limit, upper_limit, axis=1)
        
        # scale data using the standard scaler object
        temp.reset_index(drop=True, inplace=True)
        temp[feature_set] = pd.DataFrame(transformer.transform(temp[feature_set]), columns=feature_set)
        
        # return processed dataframe
        return temp


    def model_scoring_redec(self,df, final_features, logreg_model):
        temp_df = df.copy()

        # define model probability bins
        def buckets(x):
            if x > 0.58:
                return 1
            if x > 0.46:
                return 2
            if x > 0.35:
                return 3
            if x > 0.20:
                return 4
            return 5

        # score dataset using the model
        temp_df['redec_proba'] = logreg_model.predict_proba(temp_df[final_features])[:,1:].flatten()

        # generate novo score
        temp_df['redec_score'] = 1000*(1-temp_df['redec_proba'])

        # get bins for the score
        temp_df['redec_bin'] = temp_df['redec_proba'].apply(buckets)

        # return temp_df
        return temp_df

        