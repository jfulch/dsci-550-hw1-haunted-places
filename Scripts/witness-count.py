import pandas as pd
import number_parser
import re

witness_keywords_to_search = [
    'report', 'witness', 'sight', 'eyewitness', 
    'observe', 'presence', 'present', 'account', 'testimony', 'encounter', 'experience', 'see', 'saw']
pattern = '|'.join(f"{word}\\w*" for word in witness_keywords_to_search)  # This matches word + any word characters after

df_haunted = pd.read_csv("Datasets/haunted_places.tsv", sep='\t')

def parse_witness_count(description, witness_keywords_pattern=pattern):
    try:
        if pd.isna(description):
            return 0
            
        description_lower = description.lower()
        words = description_lower.split()
        
        # Find all matches and their indices
        for i, word in enumerate(words):
            if re.search(witness_keywords_pattern, word, re.IGNORECASE):
                # Check if next word is "by"
                if i + 1 < len(words) and words[i + 1] == "by":
                    # Attempt to parse the number after "by"
                    try:
                        witness_count = number_parser.parse(words[i + 2])
                        return int(witness_count)
                    except (ValueError, IndexError):
                        pass  # Parsing failed, continue searching
                
                # Check previous word for number (ie 10 witnesses, 20 people, etc)
                if i-1 >= 0 and len(words[i-1]) < 4: #there's one instance where the word before the match is a year so < 4 accounts for that
                    if words[i-1].isdigit():
                        return int(words[i-1])
                    else:
                        try:
                            witness_count = number_parser.parse(words[i-1])
                            return int(witness_count)
                        except (ValueError, IndexError):
                            pass
                return 1  # If no specific count found but word matched
        return 0  # No matches found
    except Exception:
        return 0

df_haunted['witness_count'] = df_haunted['description'].apply(lambda x: parse_witness_count(x, pattern))

# Output the updated dataframe to a TSV file
output_file = "Datasets/haunted_places_witness_count.tsv"
df_haunted.to_csv(output_file, sep='\t', index=False)

print(f"Updated data has been saved to {output_file}")

# Display the dataframe (optional, for verification)
print(df_haunted)
