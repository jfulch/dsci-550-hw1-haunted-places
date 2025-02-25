import pandas as pd
import re
from collections import Counter

def count_word_occurrences(filename, top_n=10):
    df = pd.read_csv(filename)
    
    # Concatenate all rows in the column into a single string
    all_text = ' '.join(df["description"].dropna().astype(str)).lower()
    
    # Tokenize the text into words using regex
    words = re.findall(r'\b\w+\b', all_text)

    # Define a set of stop words to exclude
    stop_words = set([
        'the', 'a', 'an', 'and', 'or', 'in', 'on', 'at', 'to', 'from', 'of',
        'is', 'are', 'was', 'were', 'it', 'that', 'this', 'these', 'those',
        'i', 'me', 'my', 'we', 'us', 'our', 'you', 'your', 'he', 'him', 'his',
        'she', 'her', 'hers', 'they', 'them', 'their', 'theirs', 'but', 'for',
        'with', 'as', 'so', 'if', 'then', 'by', 'be', 'been', 'being', 'have',
        'has', 'had', 'do', 'does', 'did', 'can', 'could', 'should', 'would', 'there', 'where',
        'about', 'over', 'under', 'above', 'below', 'up', 'down', 'out', 'in', 'inside', 'outside',
        'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten', 's', 'when'
    ])

    # Count word occurrences, excluding stop words
    filtered_words = [word for word in words if word not in stop_words]
    word_counts = Counter(filtered_words)
    
    # Get the top N words
    most_common_words = word_counts.most_common(top_n)

    # Convert the counter into a DataFrame
    word_counts_df = pd.DataFrame(most_common_words, columns=['Word', 'Count'])

    # Sort by count in descending order (already sorted by most_common, but keeping for clarity)
    word_counts_df = word_counts_df.sort_values(by='Count', ascending=False).reset_index(drop=True)
    word_counts_df.to_csv("word_counts.csv", index=False)
    return word_counts_df

if __name__ == "__main__":
    filename = "../Datasets/haunted_places.csv"  # Replace with your actual filename
    result_df = count_word_occurrences(filename)
    print(result_df.head(100))