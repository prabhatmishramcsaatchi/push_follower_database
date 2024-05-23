import boto3
import pandas as pd
from sqlalchemy import create_engine
import io
from datetime import datetime 
import json  
import os
from io import BytesIO
import numpy as np
              
# Constants (ideally these should be environment variables)
SOURCE_BUCKET = 'wikitablescrapexample'
FOLLOWER_KEY = 'amazon_sprinklr_pull/follower/'
SHEET_NAME = 'sheet'
DB_ENDPOINT = 'prod-fluency-sprinklr-db-eu2.cpmfagf94hdi.eu-west-2.rds.amazonaws.com'
DB_NAME = 'sprinklrproddb'
DB_USER = 'tableausprinklr'
DB_PASSWORD = 'HiXy074Hi'
DATA_TABLE = 'follower_table'
  
country_mapping_key = 'amazon_sprinklr_pull/mappingandbenchmark/account_metadataa.xlsx'
file_to_ingest='Follower_Data_7_FluencyWeekly.json'

   
 

def read_excel_from_s3(bucket, key, sheet_name):
    s3 = boto3.client('s3')  # Initialize an S3 client if not already done

    s3_object = s3.get_object(Bucket=bucket, Key=key)
    excel_data = s3_object['Body'].read()
    
    # Use io.BytesIO for Excel files
    excel_buffer = io.BytesIO(excel_data)
    
    # Read the Excel file from the buffer
    data = pd.read_excel(excel_buffer, sheet_name=sheet_name)
    
    return data
 
   
def merge_account_column(main_df, country_mapping_df):
    if main_df is not None and country_mapping_df is not None:
        # Check if necessary columns are present
        required_main_cols = {'Social Network', 'Original Name'}
        required_country_mapping_cols = {'Official Name', 'Platform', 'Account'}
  
        if required_main_cols.issubset(main_df.columns) and required_country_mapping_cols.issubset(country_mapping_df.columns):

            # Create a concatenated and formatted column in both DataFrames
            main_df['formatted_account'] = (main_df['Original Name'] + main_df['Social Network']).str.replace(' ', '').str.lower()
 
            country_mapping_df['formatted_account'] = (country_mapping_df['Official Name'] + country_mapping_df['Platform']).str.replace(' ', '').str.lower()

            # Create a mapping from the formatted account to the Interim Account
            account_mapping = dict(zip(country_mapping_df['formatted_account'], country_mapping_df['Account']))

            # Update the Account column in main_df where there is a match, keep original where there is no match
            main_df['Account'] = main_df['formatted_account'].map(account_mapping)
            

                

            # Drop the temporary formatted_account column
            main_df.drop(columns=['formatted_account'], inplace=True)


            return main_df
        else:
            return main_df
    else:
        return main_df

   
def aggregate_and_enrich_dataframe(df):
    # Check if both required columns exist in the DataFrame
    if 'Account' in df.columns and 'Social Network' in df.columns:
        # Create the 'Matcher' column by concatenating 'Account' and 'Social Network'
        df['Matcher'] = df['Account'] + '#' + df['Social Network']
        
        # Replace zeros with NaN to avoid interpolation issues
        df.replace(0, np.nan, inplace=True)
        
        # Set 'Date' as the index
        df = df.set_index('Date')
        
        df['Followers (SUM)'] = pd.to_numeric(df['Followers (SUM)'], errors='coerce')
        
        # Create a pivot table with 'Date' as index, 'Matcher' as columns, and summing up 'Followers (SUM)'
        df_pivot = pd.pivot_table(df, index=df.index, columns='Matcher', values='Followers (SUM)', aggfunc='sum')
        
        df['Followers (SUM)'] = pd.to_numeric(df['Followers (SUM)'], errors='coerce')
        
        # Replace zeros with NaN in the pivot table and interpolate missing values
        df_pivot.replace(0, np.nan, inplace=True)
        df_pivot = df_pivot.interpolate(method='linear', axis=0).interpolate(method='linear', axis=1)
        
        # Optionally, if you need to flatten the pivot table back to a normal DataFrame and reset the index
        df_flattened = df_pivot.reset_index().melt(id_vars=['Date'], var_name='Matcher', value_name='Followers (SUM)')
        # Split 'Matcher' back to 'Account' and 'Social Network' if necessary
        df_flattened[['Account', 'Social Network']] = df_flattened['Matcher'].str.split('#', expand=True)
        #df_flattened.loc[df_flattened['Account'] == 'Amazon IG - IN', 'Account'] = 'Amazon News IG - IN'
        
        # Reorder or select columns as needed
        final_df = df_flattened[['Date', 'Account', 'Social Network', 'Followers (SUM)']]
       
        
        
    else:
        print("Required columns ('Account', 'Social Network') are missing from the DataFrame.")
        final_df = pd.DataFrame()  # Return an empty DataFrame or handle as needed
    
    return final_df
      
def lambda_handler(event, context):
    # Extract the key (file path) from the event
    key_event = event['Records'][0]['s3']['object']['key']
    #key_event='amazon_sprinklr_pull/follower/Follower_Data_7_FluencyWeekly.json'
    
    s3 = boto3.client('s3')
 
    try:
        # Check if the event key ends with the JSON file name
        if key_event.endswith(file_to_ingest):
            # Process the line-delimited JSON file
            response = s3.get_object(Bucket=SOURCE_BUCKET, Key=key_event)
            file_content = response['Body'].read().decode('utf-8')
            data = [json.loads(line) for line in file_content.strip().split('\n')]
            df = pd.DataFrame(data)
            df=df[['DATE','ACCOUNT','SOCIAL_NETWORK','FOLLOWERS__SUM']]
            df['DATE']= pd.to_datetime(df['DATE'], unit='ms').dt.date
            
               
             
            df.rename(columns={
                            'DATE': 'Date',
                            'ACCOUNT': 'Original Name',
                            'SOCIAL_NETWORK': 'Social Network',
                            'FOLLOWERS__SUM': 'Followers (SUM)'
                     }, inplace=True)
           
            country_mapping = read_excel_from_s3(SOURCE_BUCKET,country_mapping_key, "Sheet1")
                            # Save the result to S3
                             
         
  
            df_final = merge_account_column(df, country_mapping)
            
     
            df_final=aggregate_and_enrich_dataframe(df_final)
            
            csv_buffer = BytesIO()
            df_final.to_csv(csv_buffer, index=False)
          
            key = 'bmappingmasterbenchmarks_{min_pull_date}_{max_pull_date}.csv'
            s3.put_object(Bucket=SOURCE_BUCKET , Key=key, Body=csv_buffer.getvalue())

   
        else:
            # If it's not the JSON file, you can handle other file types here
            return {
                'statusCode': 200,
                'body': 'No relevant JSON file to process'
            }
    
        # Establish database connection
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_ENDPOINT}/{DB_NAME}"
        engine = create_engine(connection_string)
   
        #Write data to PostgreSQL
        df_final.to_sql(DATA_TABLE, engine, if_exists='append', index=False)
            
                # Save the result to S3
        csv_buffer = BytesIO()
        df_final.to_csv(csv_buffer, index=False)
          
        key = 'masterbenchmarks_{min_pull_date}_{max_pull_date}.csv'
        s3.put_object(Bucket=SOURCE_BUCKET , Key=key, Body=csv_buffer.getvalue())

        return {
            'statusCode': 200,
            'body': 'Data successfully written to the database'
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': 'Error occurred during processing'
        }            
