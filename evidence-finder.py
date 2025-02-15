import pandas as pd
import datefinder
from datetime import datetime
import number_parser

def add_evidence_columns(input_file_path, output_file_path, audio_keywords, visual_keywords, event_keywords, apparition_keywords, time_keywords, witness_keywords):

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
            lambda description: parse_witness_count(description, witness_keywords) if isinstance(description, str) else 0
        )
        
        # Create the 'time_of_day' column
        df['time_of_day'] = df['description'].apply(
            lambda description: discern_time_of_day(description, time_keywords) if isinstance(description, str) else "Unknown"
        )

        # Create the 'apparition_type' column
        df['apparition_type'] = df['description'].apply(
            lambda description: discern_type(description, apparition_keywords) if isinstance(description, str) else "Unknown"
        )

        # Create the 'event_type' column
        df['event_type'] = df['description'].apply(
            lambda description: discern_type(description, event_keywords) if isinstance(description, str) else "Unknown"
        )

        # Save the updated DataFrame to a new TSV file
        df.to_csv(output_file_path, sep='\t', index=False)
        print(f"Successfully created {output_file_path} with 'audio_evidence', 'visual_evidence', 'haunted_places_date', 'haunted_places_witness_count', 'time_of_day', 'apparition_type', and 'event_type' columns.")

    except FileNotFoundError:
        print(f"Error: File not found at {input_file_path}")
    except pd.errors.EmptyDataError:
        print(f"Error: The file at {input_file_path} is empty.")
    except KeyError:
        print("Error: 'description' column not found in the file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def parse_witness_count(description, witness_keywords):

    try:
        description_lower = description.lower()
        
        # Check for any of the specified witness keywords in the description
        if any(keyword in description_lower for keyword in witness_keywords):
            words = description_lower.split()
            
            for i, word in enumerate(words):
                # If the word is a witness keyword
                if word in witness_keywords:
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

def discern_time_of_day(description, time_keywords):
    description_lower = description.lower()
    for time_period, keywords in time_keywords.items():
        if any(keyword in description_lower for keyword in keywords):
            return time_period
    return "Unknown"

def discern_type(description, keywords_dict):
    description_lower = description.lower()
    
    for type_name, keywords in keywords_dict.items():
        if any(keyword in description_lower for keyword in keywords):
            return type_name
    return "Unknown"

# Example usage:
if __name__ == "__main__":
    input_file = 'haunted_places.tsv'
    output_file = 'haunted_places_evidence.tsv'
    audio_keywords_to_search = ['noises', 'sound', 'voices']  # Example audio keywords
    visual_keywords_to_search = ['camera', 'pictures', "visual"]  # Example visual keywords
    event_keywords_to_search = {
        "Murder": ["murder", "killed", "stabbed"],
        "Death": ["died", "drowned", "passed away"],
        "Supernatural Phenomenon": ["haunted", "ghostly", "paranormal"]
    }
    apparition_keywords_to_search = {
        "Ghost": ["ghost", "specter", "phantom"],
        "Orb": ["orb", "light ball"],
        "UFO": ["ufo", "unidentified flying object"],
        "UAP": ["uap", "unidentified aerial phenomena"]
    }
    time_keywords_to_search = {
        "Morning": ["morning", "sunrise"],
        "Evening": ["evening", "night", "sunset", "dark"],
        "Dusk": ["dusk"]
    }
    witness_keywords_to_search = ["witness", "witnessed", "seen", "saw"]
    add_evidence_columns(input_file, output_file, audio_keywords_to_search, visual_keywords_to_search, event_keywords_to_search, apparition_keywords_to_search, time_keywords_to_search, witness_keywords_to_search)