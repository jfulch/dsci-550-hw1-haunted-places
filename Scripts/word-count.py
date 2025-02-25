import pandas as pd
import nltk
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger', download_dir='/Users/jfulch/nltk_data')
from nltk import pos_tag
from nltk.corpus import stopwords, wordnet as wn

def get_word_category(word, pos_tag):
    # Convert POS tag to WordNet POS tag format
    pos_map = {
        'NN': wn.NOUN, 'NNS': wn.NOUN, 'NNP': wn.NOUN, 'NNPS': wn.NOUN,
        'VB': wn.VERB, 'VBD': wn.VERB, 'VBG': wn.VERB, 'VBN': wn.VERB, 
        'VBP': wn.VERB, 'VBZ': wn.VERB,
        'JJ': wn.ADJ, 'JJR': wn.ADJ, 'JJS': wn.ADJ,
        'RB': wn.ADV, 'RBR': wn.ADV, 'RBS': wn.ADV
    }
    
    wn_pos = pos_map.get(pos_tag, None)
    if not wn_pos:
        return 'other' 
    
    # Get synsets (sets of synonyms) for the word
    synsets = wn.synsets(word, pos=wn_pos)
    if not synsets:
        return 'other'
    
    # Get the most common synset
    synset = synsets[0]
    
    # Get all hypernyms 
    hypernyms = []
    for hypernym_path in synset.hypernym_paths():
        hypernyms.extend(hypernym_path)
    
    # Define categories based on WordNet hierarchy
    categories = {
        'perception_verb': {'perceive.v.01', 'sense.v.01', 'hear.v.01', 'see.v.01'},
        'motion_verb': {'move.v.01', 'travel.v.01', 'go.v.01'},
        'communication_verb': {'communicate.v.01', 'tell.v.01', 'say.v.01'},
        'emotion_verb': {'feel.v.01', 'emotion.n.01'},
        'building': {'building.n.01', 'structure.n.01'},
        'person': {'person.n.01', 'human.n.01'},
        'location': {'location.n.01', 'place.n.01'},
        'supernatural': {
            'supernatural_being.n.01', 'spirit.n.01', 'ghost.n.01', 
            'apparition.n.01', 'phantom.n.01', 'specter.n.01', 
            'supernatural.n.01', 'demon.n.01', 'angel.n.01'
        }
    }
    
    # Check if the word itself is in any target categories
    synsets = wn.synsets(word, pos=wn_pos)
    if synsets:
        for synset in synsets:
            for category, marker_synsets in categories.items():
                if synset.name() in marker_synsets:
                    return category
        
    # Check hypernyms if no direct match
    if synsets:
        synset = synsets[0]
        hypernyms = []
        for hypernym_path in synset.hypernym_paths():
            hypernyms.extend(hypernym_path)
        
        for category, marker_synsets in categories.items():
            if any(h.name() in marker_synsets for h in hypernyms):
                return category
        return 'other'

def count_word_occurrences(filename):
    df = pd.read_csv(filename)
    
    # Concatenate all rows in the column into a single string
    all_text = ' '.join(df["description"].dropna().astype(str)).lower()
    
    # Tokenize the text into words
    words = all_text.split()

    # Remove "noise" stop words and non-alphanumeric words
    stop_words = set(stopwords.words('english'))
    
    # Create a dictionary to count word occurrences
    word_counts = {}
    for word in words:
        if not word.isalnum() or word.lower() in stop_words:
            continue
        elif word.isalnum():
            word_counts[word] = word_counts.get(word, 0) + 1
    
    # Convert the dictionary into a DataFrame
    word_counts_df = pd.DataFrame(list(word_counts.items()), columns=['Word', 'Count'])

    # Add POS tags
    pos_tags = {word: pos_tag([word])[0][1] for word in word_counts_df['Word']}
    word_counts_df['POS'] = word_counts_df['Word'].map(pos_tags)
    
    # Add semantic categories using WordNet
    word_counts_df['Category'] = word_counts_df.apply(
        lambda row: get_word_category(row['Word'].lower(), row['POS']), axis=1
    )
    
    # Sort by count in descending order
    word_counts_df = word_counts_df.sort_values(by='Count', ascending=False).reset_index(drop=True)
    word_counts_df.to_csv("word_counts.csv", index=False)
    return word_counts_df

if __name__ == "__main__":
    filename = "../Datasets/haunted_places.csv"  # Replace with your actual filename
    result_df = count_word_occurrences(filename)
    print(result_df.head())  # Print the first few rows of the DataFrame