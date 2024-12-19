import pandas as pd
import psutil

def print_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    print(f"Memory usage: {mem_info.rss / (1024 ** 2):.2f} MB")

def process_in_chunks(df, chunk_size):
    for start in range(0, len(df), chunk_size):
        yield df[start:start + chunk_size]

print("Initial memory usage:")
print_memory_usage()

print("Loading portal_base_df...")
portal_base_df = pd.read_pickle("/workspaces/portal_base/data/processed/portal_base_trimmed.pkl")
print("portal_base_df loaded successfully.")
print(f"portal_base_df shape: {portal_base_df.shape}")
print(f"portal_base_df columns: {portal_base_df.columns}")
print_memory_usage()

print("Loading sioe_df...")
sioe_df = pd.read_pickle("/workspaces/portal_base/data/processed/sioe_base.pkl")
print("sioe_df loaded successfully.")
print(f"sioe_df shape: {sioe_df.shape}")
print(f"sioe_df columns: {sioe_df.columns}")
print_memory_usage()

nif_no_match_list = []
chunk_size = 100000

for i, portal_base_chunk in enumerate(process_in_chunks(portal_base_df, chunk_size)):
    print(f"Processing chunk {i+1}...")
    print(f"portal_base_chunk shape: {portal_base_chunk.shape}")
    
    print("Merging dataframes on 'nif_adjudicante' and ' NIPC'...")
    nif_merged_df = pd.merge(
        portal_base_chunk,
        sioe_df,
        left_on='nif_adjudicante',
        right_on=' NIPC',
        how='left',
        indicator=True
    )
    print("Merge completed.")
    print(f"nif_merged_df shape: {nif_merged_df.shape}")
    print_memory_usage()

    print("Filtering rows with no match in 'nif_adjudicante'...")
    nif_no_match = nif_merged_df[nif_merged_df['_merge'] == 'left_only']
    print(f"Number of rows with no match: {len(nif_no_match)}")
    
    nif_no_match_list.append(nif_no_match[portal_base_chunk.columns])

print("Concatenating all no match chunks...")
subset_no_match_nif = pd.concat(nif_no_match_list)
print(f"subset_no_match_nif shape: {subset_no_match_nif.shape}")
print_memory_usage()

print("Saving the subset as pickle...")
subset_no_match_nif.to_pickle("/workspaces/portal_base/data/processed/portal_base_no_match_nif.pkl")
print("Subset saved successfully.")
print_memory_usage()