from lifebear-juliett-functions import *

#Step 1: De-duplicate the CSV file based on specified columns
process_duplicates_csv('lifebear.csv', 'valid_data.csv', 'lifebear_juliettSPB_duplicates.csv', sep=';', columns=['login_id', 'mail_address'])

# Step 2: Split CSVs into chunks 
#split_csv_into_chunks('/content/lifebear.csv', 1000000, '/content/chunks', sep=';') # Split original file into chunks before processing
split_csv_into_chunks('valid_data.csv', 1000000, './chunks', sep=',') # Split de-duplicated file into chunks 

# Step 3: Run Function for data cleaning and cplace the valid and garbage chunks into two folders for analysis or further processing
process_chunked_csvs_output_folders('./chunks', './cleaned_chunks', './error_chunks')

# Step 3 (Alternative): Run Function for data cleaning and combining the valid and garbage chunks into two final files
process_chunked_csvs('./chunks', 'lifebear_juliettSPB_clean.csv', 'lifebear_juliettSPB_garbage.csv')

# Step 4 (Optional): Combine chunks into a single CSV
#combine_csv_chunks('./cleaned_chunks', 'lifebear_juliettSPB_clean.csv')