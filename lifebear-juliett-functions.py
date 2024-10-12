# prompt: Create a function to get chunked csvs from a specified folder, runs the validation functions and merges the chunks into a specified final valid csv file and final error csv file. INclude error checking and logging.

import csv
import pandas as pd
import re
import unicodedata
import os
import logging
import datetime as dt


# prompt: Create a function to remove duplicate records based on specified columns. Add the duplicates records to separate dataframe and drop them from the original. Include error checking and logging.

def remove_duplicate_records(df, columns):
    """
    Removes duplicate records based on specified columns.
    Adds duplicate records to a separate dataframe and drops them from the original.

    Args:
        df (pd.DataFrame): The dataframe to process.
        columns (list): A list of column names to consider for duplicate detection.

    Returns:
        tuple: A tuple containing the updated dataframe with unique records
               and a new dataframe with duplicate records.
    """
    try:
        duplicate_df = pd.DataFrame()
        df_deduplicated = df.drop_duplicates(subset=columns, keep='first')
        duplicate_rows = df[~df.index.isin(df_deduplicated.index)]

        if not duplicate_rows.empty:
            duplicate_df = pd.concat([duplicate_df, duplicate_rows], ignore_index=True)

        print(f"Duplicate removal complete. Duplicate records appended to duplicate_df.")
        return df_deduplicated, duplicate_df

    except Exception as e:
        print(f"Error occurred during duplicate removal: {e}")
        return df, pd.DataFrame()


# prompt: Create a function to split a large csv into chunks in a specified folder or path using the chunksize parameter in read_csv

def split_csv_into_chunks(file_path, chunksize, output_directory, sep=','):
  """Splits a large CSV file into smaller chunks using the chunksize parameter.

  Args:
    file_path: The path to the large CSV file.
    chunksize: The number of rows per chunk.
    output_directory: The directory where the chunks should be saved.
  """
  try:
    if not os.path.exists(output_directory):
      os.makedirs(output_directory)

    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize, sep=sep)):
      output_file = os.path.join(output_directory, f"chunk_{i+1}.csv")
      chunk.to_csv(output_file, index=False)

    print(f"File '{file_path}' split into {i+1} chunks in '{output_directory}'.")

  except FileNotFoundError as e:
    print(f"Error: {e}")
  except Exception as e:
    print(f"An unexpected error occurred: {e}")


# prompt: Create a function to remove the time from a date in specified columns

def remove_time_from_date(df, columns):
  """Removes the time component from date columns in a DataFrame.

  Args:
    df: The DataFrame containing the date columns.
    columns: A list of column names to process.

  Returns:
    The DataFrame with the time component removed from the specified columns.
  """
  try:
    for column in columns:
      if column in df.columns:
        # Convert to datetime if not already
        df[column] = pd.to_datetime(df[column], errors='coerce')
        # Remove the time component
        df[column] = df[column].dt.date

    return df
  except Exception as e:
    print(f"An error occurred: {e}")
    return df


# prompt: create a function to validate emails addresses in a dataframe, append the records with an invalid email address to a dataframe and drop them from the original dataframe. Return both the updated dataframe and the error dataframe. Include error checking and loggging.

def validate_and_remove_invalid_emails(df, email_column):
    """
    Validates email addresses in a dataframe, appends records with invalid email
    addresses to a new dataframe, and removes them from the original dataframe.

    Args:
        df (pd.DataFrame): The dataframe containing email addresses.
        email_column (str): The name of the column containing email addresses.

    Returns:
        tuple: A tuple containing the updated dataframe with valid email addresses
            and a new dataframe with records containing invalid email addresses.
    """
    try:
        # Regular expression for basic email validation
        email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

        # Create a new dataframe to store records with invalid email addresses
        error_df = pd.DataFrame()

        # Iterate through the dataframe and validate email addresses
        for index, row in df.iterrows():
            email = row[email_column]
            if not re.match(email_regex, email):
                # Append record to the error dataframe
                error_df = pd.concat([error_df, pd.DataFrame([row])], ignore_index=True)
                # Drop the record from the original dataframe
                df.drop(index, inplace=True)

        print("Validation complete. Invalid email records appended to error_df.")
        return df, error_df

    except Exception as e:
        print(f"Error occurred during email validation: {e}")
        return df, pd.DataFrame()  # Return empty error dataframe in case of error


# prompt: Create a function to get chunked csvs from a specified folder, runs the validation functions and merges the chunks into a specified final valid csv file and final error csv file. INclude error checking and logging.

def process_chunked_csvs(input_folder, output_valid_csv, output_error_csv, email_column_name='mail_address', date_columns=['created_at']):
  """
  Processes chunked CSV files from a specified folder, runs validation functions,
  and merges the results into final valid and error CSV files.

  Args:
      input_folder (str): The path to the folder containing chunked CSV files.
      output_valid_csv (str): The path to the output CSV file for valid records.
      output_error_csv (str): The path to the output CSV file for error records.
      output_duplicates_csv (str): The path to the output CSV file for duplicate records.
      email_column_name (str): The name of the email column.
      date_columns (list): A list of column names to consider for date validation.
  """

  try:
    # Setup logging
    logging.basicConfig(filename='processing_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    valid_df = pd.DataFrame()
    error_df = pd.DataFrame()

    for filename in os.listdir(input_folder):
      if filename.endswith(".csv"):
        file_path = os.path.join(input_folder, filename)
        logging.info(f"Processing file: {file_path}")

        try:
          df = pd.read_csv(file_path, low_memory=True)

          # Run validation functions
          #df['gender'] = df['gender'].replace({0.0: 'M', 1.0: 'F'})
          df, chunk_error_df = validate_and_remove_invalid_emails(df, email_column_name)
          df = remove_time_from_date(df, date_columns)
          #df['gender'] = df['gender'].astype(int)
          #df['birthday_on'] = df['birthday_on'].dt.date
          # df = truncate_large_fields(df)
          # df = remove_non_utf_characters(df)

          valid_df = pd.concat([valid_df, df], ignore_index=True)

          # Concatenate error dataframes
          #chunk_error_df = pd.concat([chunk_error_df,chunk_dup_error_df], ignore_index=True)
          error_df = pd.concat([error_df, chunk_error_df], ignore_index=True)

          logging.info(f"File {file_path} processed successfully.")

        except Exception as e:
          logging.error(f"Error processing file {file_path}: {e}")

    # Remove duplicates from full dataframe
    #valid_df, duplicates_df = remove_duplicate_records(valid_df, ['mail_address']) #remove duplicates based on email
    #error_df = pd.concat([error_df, chunk_dup_error_df], ignore_index=True)

    # Save final dataframes
    valid_df.to_csv(output_valid_csv, index=False)
    error_df.to_csv(output_error_csv, index=False)
    #duplicates_df.to_csv(output_duplicates_csv, index=False)

    logging.info(f"Final valid data saved to {output_valid_csv}.")
    logging.info(f"Final error data saved to {output_error_csv}.")
    #logging.info(f"Final duplicates data saved to {output_duplicates_csv}.")

  except Exception as e:
    logging.critical(f"Critical error during processing: {e}")

# prompt: Create a function that will combine csv chunks from a specified folder into one csv file

def combine_csv_chunks(input_folder, output_file):
  """
  Combines multiple CSV chunks from a folder into a single CSV file.

  Args:
    input_folder: The path to the folder containing CSV chunks.
    output_file: The path to the output CSV file.
  """

  combined_df = pd.DataFrame()
  for filename in os.listdir(input_folder):
    if filename.endswith(".csv"):
      file_path = os.path.join(input_folder, filename)
      try:
        df = pd.read_csv(file_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
      except Exception as e:
        print(f"Error reading file {file_path}: {e}")

  combined_df.to_csv(output_file, index=False)
  print(f"Combined CSV chunks saved to {output_file}")