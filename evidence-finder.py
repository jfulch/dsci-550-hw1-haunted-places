import pandas as pd
import datefinder
from datetime import datetime
import number_parser

def add_evidence_columns(input_file_path, output_file_path, audio_keywords, visual_keywords):

    try:
        df = pd.read_csv(input_file_path, sep='\t')

        # Create the 'audio_evidence' column
        df['audio_evidence'] = df['description'].apply(
            lambda description: isinstance(description, str) and any(keyword in description.lower() for keyword in audio_keywords)
        )

        # Create the 'visual_evidence' column
        df['visual_evidence'] = df['description'].apply(
            lambda description: isinstance(description, str) and any(keyword in description.lower() for keyword in visual_keywords)
        )

        # Create the 'haunted_places_date' column
        df['haunted_places_date'] = df['description'].apply(
            lambda description: next(datefinder.find_dates(description), datetime(2025, 1, 1)).strftime('%Y/%m/%d')
            if isinstance(description, str) and any(datefinder.find_dates(description))
            else datetime(2025, 1, 1).strftime('%Y/%m/%d')
        )

        # Create the 'haunted_places_witness_count' column
        df['haunted_places_witness_count'] = df['description'].apply(
            lambda description: parse_witness_count(description) if isinstance(description, str) else 0
        )
        
        # Create the 'time_of_day' column
        df['time_of_day'] = df['description'].apply(
            lambda description: discern_time_of_day(description) if isinstance(description, str) else "Unknown"
        )

        # Save the updated DataFrame to a new TSV file
        df.to_csv(output_file_path, sep='\t', index=False)
        print(f"Successfully created {output_file_path} with 'audio_evidence', 'visual_evidence', 'haunted_places_date', 'haunted_places_witness_count', and 'time_of_day' columns.")

    except FileNotFoundError:
        print(f"Error: File not found at {input_file_path}")
    except pd.errors.EmptyDataError:
        print(f"Error: The file at {input_file_path} is empty.")
    except KeyError:
        print("Error: 'description' column not found in the file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def parse_witness_count(description):

    try:
        # Look for phrases like "seen by X people" or "witnessed by X people"
        description_lower = description.lower()
        if "witness" in description_lower or "seen by" in description_lower:
            # Split the description into words
            words = description_lower.split()
            
            # Iterate through the words to find potential witness counts
            for i, word in enumerate(words):
                if word in ["witnessed", "seen"]:
                    # Check if the next word is "by"
                    if i + 1 < len(words) and words[i + 1] == "by":
                        # Attempt to parse the number after "by"
                        try:
                            witness_count = number_parser.parse(words[i + 2])
                            return int(witness_count)
                        except (ValueError, IndexError):
                            pass  # Parsing failed, continue searching
                else:
                    try:
                        witness_count = number_parser.parse(word)
                        if isinstance(witness_count, int):
                            return int(witness_count)
                    except (ValueError, IndexError):
                        pass
        return 0  # No witness count found
    except Exception:
        return 0

def discern_time_of_day(description):

    description_lower = description.lower()
    if any(keyword in description_lower for keyword in ["morning", "sunrise"]):
        return "Morning"
    elif any(keyword in description_lower for keyword in ["evening", "night", "sunset", "dark"]):
        return "Evening"
    elif "dusk" in description_lower:
        return "Dusk"
    else:
        return "Unknown"

# Example usage:
if __name__ == "__main__":
    input_file = 'haunted_places.tsv'
    output_file = 'haunted_places_evidence.tsv'
    audio_keywords_to_search = ['noises', 'sound', 'voices']  # Example audio keywords
    visual_keywords_to_search = ['camera', 'pictures', "visual"]  # Example visual keywords
    add_evidence_columns(input_file, output_file, audio_keywords_to_search, visual_keywords_to_search)