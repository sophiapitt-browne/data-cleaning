# Lifebear Data Cleaning

# Data Cleaning and Processing Pipeline

This pipeline is designed to clean and process large CSV files using Python and the Pandas library. It includes functions for handling duplicates, splitting data into chunks, validating emails, and cleaning date formats.

## Functions

### `remove_duplicate_records(df, columns)`

Removes duplicate records from a DataFrame based on specified columns. It adds the duplicate records to a separate DataFrame and drops them from the original.

### `process_duplicates_csv(file_path, output_valid_csv, output_duplicates_csv, columns, sep=',')`

Processes a single CSV file, removes duplicates based on specified columns, and outputs valid and duplicate data to separate CSV files.

### `split_csv_into_chunks(file_path, chunksize, output_directory, sep=',')`

Splits a large CSV file into smaller chunks with a specified `chunksize` and saves them in an output directory.

### `process_chunked_csvs_output_folders(input_folder, output_valid_folder, output_error_folder, email_column_name='mail_address', date_columns=['created_at'])`

Processes chunked CSV files, validates email addresses, removes time from date columns, and saves the cleaned and error chunks into separate folders.

### `remove_time_from_date(df, columns)`

Removes the time component from specified date columns in a DataFrame.

### `validate_and_remove_invalid_emails(df, email_column)`

Validates email addresses in a DataFrame and removes invalid ones, appending them to a separate error DataFrame.

### `process_chunked_csvs(input_folder, output_valid_csv, output_error_csv, email_column_name='mail_address', date_columns=['created_at'])`

Processes chunked CSV files, validates email addresses, removes time from date columns, and merges the results into final valid and error CSV files.

### `combine_csv_chunks(input_folder, output_file)`

Combines multiple CSV chunks from a folder into a single CSV file.

### `remove_invalid_rows(input_file, output_file, delimiter=',')`

Removes invalid rows from a CSV file based on the header and the expected number of columns.

### `remove_invalid_rows_from_csv(csv_file_path, delimiter=',')`

Removes invalid rows (rows with an invalid number of columns) from a CSV file.

## Function Calls

The pipeline is executed through a series of function calls, including:

- `process_duplicates_csv`: Identifies and separates duplicate records in the original CSV file.
- `split_csv_into_chunks`: Splits the valid data into smaller chunks for easier processing.
- `process_chunked_csvs_output_folders` or `process_chunked_csvs`: Cleans and validates the chunks, either saving them to separate folders or merging them into final output files.
- `combine_csv_chunks`: (Optional) Combines cleaned chunks into a single CSV file.


This pipeline is designed to ensure data quality and consistency by identifying and handling duplicates, validating email addresses, cleaning date formats, and addressing other data inconsistencies. The modular nature of the functions allows for easy adaptation and extension to suit different data processing needs.
