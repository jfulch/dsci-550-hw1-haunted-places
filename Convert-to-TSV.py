import pandas as pd

file_name = "haunted_places"

def convert_csv_to_tsv(input_csv, output_tsv):
    try:
        # Read the CSV file
        df = pd.read_csv(input_csv)
        
        # Write to TSV file
        df.to_csv(output_tsv, sep='\t', index=False)
        print(f"Successfully converted {input_csv} to {output_tsv}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    # Replace these with your actual file names
    input_csv_file = f"Datasets/{file_name}.csv"
    output_tsv_file = f"Datasets/{file_name}.tsv"
    
    convert_csv_to_tsv(input_csv_file, output_tsv_file)