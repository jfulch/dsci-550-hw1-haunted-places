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


def discern_type(description, keywords_dict):
    description_lower = description.lower()
    
    for type_name, keywords in keywords_dict.items():
        if any(keyword in description_lower for keyword in keywords):
            return type_name
    return "Unknown"

# Example usage:
if __name__ == "__main__":
    input_file = '../Datasets/haunted_places.tsv'
    output_file = '../Datasets/haunted_places_audio_visual_evidence.tsv'
    audio_keywords_to_search = [
    "noises", "sounds", "voices", "whispers", "screams", "cries", "moans", "groans", "laughter", "giggling", "footsteps", "knocking", "banging", "clanking", "rattling", "slamming", "splashing", "music", "singing", "applause", "gunshots",
    "chanting", "yelling", "shouting", "sobbing", "weeping", "humming", "buzzing", "thumping", "tapping", "creaking", "rustling", "scratching", "growling", "hissing", "roaring", "ringing", "ticking",
    "unexplained noise", "strange sound", "eerie sound", "faint noise", "disembodied voice", "phantom sounds", "EVP",
    "bells", "piano", "static", "echoing", "muffled", "unnatural silence"]

    visual_keywords_to_search = [
    "apparition", "figure", "shadowy figure", "shadow", "ghost", "specter", "spectre", "phantom", "wraith", "shade", "spirit", "entity", "ectoplasm",
    "orb", "mist", "glowing", "glowing eyes", "red eyes", "unexplained light", "strange light", "flickering light", "flashing light",
    "seen", "sighting", "witnessed", "observed", "glimpse", "shape", "form", "outline", "silhouette", "transparent figure", "translucent figure",
    "dematerializing", "disappearing", "vanishing", "floating figure", "hovering figure",
    "photograph", "video", "film", "image", "picture"]
    add_evidence_columns(input_file, output_file, audio_keywords_to_search, visual_keywords_to_search)