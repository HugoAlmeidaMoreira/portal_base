import pandas as pd
import concurrent.futures
from tqdm import tqdm

# Define the range of years
start_year = 2012
end_year = 2024

# Function to read a single Excel file
def read_excel_file(year):
    file_path = f"data/raw/contratospub{year}.xlsx"
    return pd.read_excel(file_path)

# Use ThreadPoolExecutor to read files in parallel
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Map the read_excel_file function to the range of years with tqdm progress bar
    df_list = list(tqdm(executor.map(read_excel_file, range(start_year, end_year + 1)), total=end_year - start_year + 1))

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(df_list, ignore_index=True)

# Display the combined DataFrame
combined_df.head()

# Save as pickle file
pkl_output_path = "data/processed/combined_data.pkl"
combined_df.to_pickle(pkl_output_path)