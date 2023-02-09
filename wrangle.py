import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pydataset import data
from scipy import stats
from env import host, username, password    # import needed for get_connection() to operate


# Function to build the connection between notebook and MySql. Will be used in other functions.
# Returns the string that is neccessary for that connection.
def get_connection(db, user = username, host = host, password = password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

#--------------------------------------------------------------------------------------------------

test = '%'
query = f'''
    select * 
    from properties_2017
    left join predictions_2017 using(id)
    left join airconditioningtype as ac on ac.`airconditioningtypeid` = properties_2017.`airconditioningtypeid`
    left join `architecturalstyletype` as ar on ar.`architecturalstyletypeid` = properties_2017.`architecturalstyletypeid`
    left join `buildingclasstype` as b on b.`buildingclasstypeid` = `properties_2017`.`buildingclasstypeid`
    left join `heatingorsystemtype` as h on h.`heatingorsystemtypeid` = properties_2017.`heatingorsystemtypeid`
    left join `propertylandusetype` as p on p.`propertylandusetypeid` = properties_2017.`propertylandusetypeid`
    left join `storytype` as s on s.`storytypeid` = `properties_2017`.`storytypeid`
    left join `typeconstructiontype` as t on t.`typeconstructiontypeid` = `properties_2017`.`typeconstructiontypeid`
    where `transactiondate` like "2017{test}{test}" 

        '''


def get_zillow():
    filename = "zillow2.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        # read the SQL query into a dataframe
        df = pd.read_sql( query , get_connection('zillow'))

        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename, index=False)

        # Return the dataframe
        return df  

#--------------------------------------------------------------------------------------------------
    
def clean_zillow(df):
# Dropped all nulls. Less than 1% of data.
    df = df.dropna()
    # Drop dupes
    df = df.drop_duplicates()

    # Drop duplicated columns
    df.drop(['parcelid','propertylandusetypeid','heatingorsystemtypeid','architecturalstyletypeid','\
    airconditioningtypeid','typeconstructiontypeid','storytypeid','buildingclasstypeid'], axis=1, inplace=True)

    return df

#--------------------------------------------------------------------------------------------------



def wrangle_zillow():
    #Acquire Zillow data
    zillow = get_zillow()
    # Reset index and drop prior index
    zillow = zillow.reset_index().drop('index',axis=1)
    # Drop dupes
    zillow = zillow.drop_duplicates()
  


    return zillow
    
#--------------------------------------------------------------------------------------------------

def missing(df):
    
    # Create df of sum of nulls
    df2 = pd.DataFrame(df.isna().sum())
    
    # Rename column
    df2.rename(columns={0: 'num_rows_missing'}, inplace=True)
    
    # Create new row using value of rows missing / length of OG df
    df2['pct_rows_missing'] = df2['num_rows_missing'] / len(df)
    
    return df2

#--------------------------------------------------------------------------------------------------

def handle_missing_values(df, prop_column_req):
    
    '''
    Function that intakes df and a proportion of a column that should not be nulls. Anything above that 
    threshold will be dropped. 
    
    prop_column_req = value between 0 and 1

    '''
    above_pct = round(len(df) * prop_column_req)
    
    ab = df.copy()
    
    ab = ab.dropna(thresh= above_pct, axis=1)
    
    return ab

    #--------------------------------------------------------------------------------------------------

def outlier_detector(df, column, k=1.5):
    
    
    q1, q3 = df[column].quantile([0.25, 0.75])
    
    iqr = q3 - q1
    
    upper_bound = q1 - k * iqr
    
    lower_bound = q1 - k * iqr
    
    print(f'{column} Upper bound: {upper_bound}, {column} Lower Bound: {lower_bound}')