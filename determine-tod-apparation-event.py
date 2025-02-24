import pandas as pd
from fuzzywuzzy import fuzz, process
from collections import Counter

def add_evidence_columns(input_file_path, output_file_path, event_keywords, apparition_keywords, time_keywords):
    # Initialize counters
    time_counter = Counter()
    apparition_counter = Counter()
    event_counter = Counter()

    try:
        df = pd.read_csv(input_file_path, sep='\t')

        # Create the 'time_of_day' column
        df['time_of_day'] = df['description'].apply(
            lambda description: discern_category(description, time_keywords, time_counter) if isinstance(description, str) else "Unknown"
        )

        # Create the 'apparition_type' column
        df['apparition_type'] = df['description'].apply(
            lambda description: discern_category(description, apparition_keywords, apparition_counter) if isinstance(description, str) else "Unknown"
        )

        # Create the 'event_type' column
        df['event_type'] = df['description'].apply(
            lambda description: discern_category(description, event_keywords, event_counter) if isinstance(description, str) else "Unknown"
        )

        # Save the updated DataFrame to a new TSV file
        df.to_csv(output_file_path, sep='\t', index=False)
        print(f"Successfully created {output_file_path} with 'time_of_day', 'apparition_type', and 'event_type' columns.")

        # Print the counts
        print("\nCounts for each category:")
        print("Time of Day:")
        for category, count in time_counter.items():
            print(f"  {category}: {count}")
        print("\nApparition Type:")
        for category, count in apparition_counter.items():
            print(f"  {category}: {count}")
        print("\nEvent Type:")
        for category, count in event_counter.items():
            print(f"  {category}: {count}")

    except FileNotFoundError:
        print(f"Error: File not found at {input_file_path}")
    except pd.errors.EmptyDataError:
        print(f"Error: The file at {input_file_path} is empty.")
    except KeyError:
        print("Error: 'description' column not found in the file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def discern_category(description, keywords_dict, counter):
    description_lower = description.lower()
    words = description_lower.split()
    
    # Exact matching
    for category, keywords in keywords_dict.items():
        if any(keyword.lower() in words for keyword in keywords):
            counter[category] += 1
            return category
    
    # Fuzzy matching
    all_keywords = [keyword for keywords in keywords_dict.values() for keyword in keywords]
    best_match = process.extractOne(description_lower, all_keywords, scorer=fuzz.partial_ratio)
    
    if best_match and best_match[1] >= 70:  # Lowered threshold to 70
        matched_keyword = best_match[0]
        for category, keywords in keywords_dict.items():
            if matched_keyword in keywords:
                counter[category] += 1
                return category
    
    counter["Unknown"] += 1
    return "Unknown"

if __name__ == "__main__":
    input_file = 'Datasets/haunted_places.tsv'
    output_file = 'Datasets/haunted_places_evidence_tod_app_event.tsv'
    
    event_keywords_to_search = {
        "Murder": ["murder", "killed", "stabbed", "slaughter", "assassin", "homicide", "massacre"],
        "Death": ["died", "drowned", "passed away", "deceased", "perished", "fatal", "mortality"],
        "Supernatural Phenomenon": ["haunted", "ghostly", "paranormal", "supernatural", "spectral", "eerie", "otherworldly", "mysterious", "unexplained"],
        "Accident": ["accident", "crash", "collision", "mishap", "disaster"],
        "War": ["war", "battle", "conflict", "combat", "military", "soldier"],
        "Natural Disaster": ["earthquake", "flood", "hurricane", "tornado", "tsunami", "volcanic eruption"]
    }
    
    apparition_keywords_to_search = {
        "Ghost": ["ghost", "specter", "phantom", "apparition", "spirit", "wraith", "poltergeist"],
        "Mist": ["mist", "ectomist", "ectoplasm", "wisp", "foggy"],
        "Partial Apparition": ["partial", "silhouette", "distorted"],
        "Full-Bodied Apparition": ["full-bodied", "solid"],
        "Ghost Light": ["light", "glowing", "sphere", "orb"],
        "Shadow Figure": ["shadow", "dark figure", "silhouette"]
    }
    
    time_keywords_to_search = {
        "Morning": ["morning", "sunrise", "dawn", "daybreak", "early", "am", "a.m.", "breakfast"],
        "Evening": ["evening", "night", "nightfall", "dark", "midnight", "pm", "p.m.", "late"],
        "Dusk": ["dusk", "twilight", "sunset", "sundown", "gloaming", "evenfall"]
    }
    
    add_evidence_columns(input_file, output_file, event_keywords_to_search, apparition_keywords_to_search, time_keywords_to_search)
