import pandas as pd
import os

def combine_tsv_files(input_files, output_file, header_option='first'):
    """
    Combines multiple TSV files into a single TSV file.
    
    Parameters:
        input_files (list): List of paths to TSV files to combine
        output_file (str): Path for the output combined TSV file
        header_option (str): Either 'first' to keep only the first file's header,
                           or 'all' to keep all headers, 
                           or 'unique' to skip duplicate headers
    """
    print(f"Combining {len(input_files)} files...")
    
    # Check if all input files exist
    for file in input_files:
        if not os.path.exists(file):
            raise FileNotFoundError(f"Input file not found: {file}")
    
    # Track columns we've already seen
    seen_columns = set()
    
    # Read and combine files
    dfs = []
    for i, file in enumerate(input_files):
        print(f"Reading file: {file}")
        
        # Read the file
        df = pd.read_csv(file, sep='\t')
        
        # Handle headers based on option
        if header_option == 'first':
            if i == 0:
                # For first file, keep all columns
                seen_columns.update(df.columns)
            else:
                # For subsequent files, drop columns we've seen before
                df = df[[col for col in df.columns if col not in seen_columns]]
                # Update our set of seen columns
                seen_columns.update(df.columns)
        elif header_option == 'unique':
            # Keep only columns we haven't seen before
            df = df[[col for col in df.columns if col not in seen_columns]]
            # Update our set of seen columns
            seen_columns.update(df.columns)
        
        # Add non-empty dataframe to our list
        if not df.empty:
            dfs.append(df)
    
    # Combine all dataframes
    if dfs:
        combined_df = pd.concat(dfs, axis=1)
        
        # Write to output file
        print(f"Writing combined data to: {output_file}")
        combined_df.to_csv(output_file, sep='\t', index=False)
        print(f"Successfully combined {len(input_files)} files into {output_file}")
        print(f"Combined file has {len(combined_df)} rows and {len(combined_df.columns)} columns")
    else:
        print("No data to combine after processing headers.")

if __name__ == "__main__":
    # Hardcoded file paths
    input_files = [
        "../Datasets/haunted_places.tsv",
        "../Datasets/haunted_places_audio_visual_evidence.tsv",
        "../Datasets/haunted_places_evidence_date.tsv",
        "../Datasets/haunted_places_evidence_daylight.tsv",
        "../Datasets/haunted_places_evidence_tod_app_event.tsv",
        "../Datasets/haunted_places_witness_count.tsv",
    ]
    
    output_file = "../Datasets/haunted_places_combined_output.tsv"
    
    # Use the 'unique' option to skip duplicate headers
    combine_tsv_files(input_files, output_file, header_option='unique')