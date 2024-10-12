# Lifebear Data Cleaning and Processing Pipeline

This pipeline is designed to clean and process large CSV files using Python and the Pandas library. It includes functions for handling duplicates, splitting data into chunks, validating emails, and cleaning date formats.

## Main Functions

### `remove_duplicate_records(df, columns)`

Removes duplicate records from a DataFrame based on specified columns. It adds the duplicate records to a separate DataFrame and drops them from the original.

**How it works:**
1. Identifies duplicate rows in the DataFrame using the `pd.DataFrame.drop_duplicates` method, considering the specified `columns`.
2. Creates a new DataFrame (`duplicate_df`) containing only the duplicate rows.
3. Removes the duplicate rows from the original DataFrame.
4. Returns the updated DataFrame (with duplicates removed) and the `duplicate_df`.

### `process_duplicates_csv(file_path, output_valid_csv, output_duplicates_csv, columns, sep=',')`

Processes a single CSV file, removes duplicates based on specified columns, and outputs valid and duplicate data to separate CSV files.

**How it works:**
1. Reads the CSV file into a Pandas DataFrame.
2. Calls `remove_duplicate_records` to identify and remove duplicates.
3. Saves the DataFrame with unique records to `output_valid_csv`.
4. Saves the DataFrame with duplicate records to `output_duplicates_csv`.

### `split_csv_into_chunks(file_path, chunksize, output_directory, sep=',')`

Splits a large CSV file into smaller chunks with a specified `chunksize` and saves them in an output directory.

**How it works:**
1. Reads the CSV file in chunks using the `pd.read_csv` function with the `chunksize` parameter.
2. Iterates through the chunks and saves each chunk to a separate CSV file in the `output_directory`.

### `process_chunked_csvs_output_folders(input_folder, output_valid_folder, output_error_folder, email_column_name='mail_address', date_columns=['created_at'])`

Processes chunked CSV files, validates email addresses, removes time from date columns, and saves the cleaned and error chunks into separate folders.

**How it works:**
1. Iterates through CSV files in the `input_folder`.
2. Reads each file into a DataFrame.
3. Calls `validate_and_remove_invalid_emails` to validate and remove invalid emails.
4. Calls `remove_time_from_date` to remove time from date columns.
5. Saves the cleaned DataFrame to the `output_valid_folder`.
6. Saves the DataFrame with errors (invalid emails) to the `output_error_folder`.

### `remove_time_from_date(df, columns)`

Removes the time component from specified date columns in a DataFrame.

**How it works:**
1. Iterates through the specified `columns`.
2. Converts the column to datetime objects using `pd.to_datetime`.
3. Extracts the date part using `dt.date` and updates the column.

### `validate_and_remove_invalid_emails(df, email_column)`

Validates email addresses in a DataFrame and removes invalid ones, appending them to a separate error DataFrame.

**How it works:**
1. Uses a regular expression to validate email addresses in the specified `email_column`.
2. Creates an empty DataFrame (`error_df`) to store records with invalid emails.
3. Iterates through the DataFrame and appends rows with invalid emails to `error_df`.
4. Removes rows with invalid emails from the original DataFrame.
5. Returns the updated DataFrame and `error_df`.

### `process_chunked_csvs(input_folder, output_valid_csv, output_error_csv, email_column_name='mail_address', date_columns=['created_at'])`

Processes chunked CSV files, validates email addresses, removes time from date columns, and merges the results into final valid and error CSV files.

**How it works:**
1. Similar to `process_chunked_csvs_output_folders`, but instead of saving to separate folders, it merges the cleaned chunks into a single `output_valid_csv` and the error chunks into a single `output_error_csv`.

### `combine_csv_chunks(input_folder, output_file)`

Combines multiple CSV chunks from a folder into a single CSV file.

**How it works:**
1. Reads each CSV file in the `input_folder` into a DataFrame.
2. Combines each Dataframe in a single Dataframe and converts it to a CSV file using `output_file`.

## Function Calls

The pipeline is executed through a series of function calls, including:

- `process_duplicates_csv`: Identifies and separates duplicate records in the original CSV file.
- `split_csv_into_chunks`: Splits the valid data into smaller chunks for easier processing.
- `process_chunked_csvs_output_folders` or `process_chunked_csvs`: Cleans and validates the chunks, either saving them to separate folders or merging them into final output files.
- `combine_csv_chunks`: (Optional) Combines cleaned chunks into a single CSV file.


This pipeline is designed to ensure data quality and consistency by identifying and handling duplicates, validating email addresses, cleaning date formats, and addressing other data inconsistencies. The modular nature of the functions allows for easy adaptation and extension to suit different data processing needs. 

## Error Checking and Logging

The pipeline adopts a proactive approach by anticipating potential issues and implementing measures to handle them gracefully. The use of error checking and logging ensures that the pipeline remains reliable and provides valuable insights into its execution.

**Graceful Handling of Errors**: The pipeline can continue running even if some errors occur, preventing complete disruption.

**Easy Debugging**: Log files offer a detailed record of events, making it easier to identify and fix issues.

**Improved Maintainability**: The code becomes more robust and maintainable due to the structured error handling and logging.

**Monitoring and Auditing**: Log files can be used for monitoring the pipeline's performance and auditing its operations.

