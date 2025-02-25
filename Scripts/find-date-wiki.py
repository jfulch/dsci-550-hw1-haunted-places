import re
import datefinder
import pandas as pd
from datetime import datetime
import dateparser
import spacy
import wikipedia
import time

# Load the spacy model
nlp = spacy.load("en_core_web_sm")

# Initialize cache
wikipedia_cache = {}

def generate_search_terms(location):
    """Generate various search terms for a location."""
    terms = [
        location,
        f"{location} history",
        f"history of {location}",
        f"{location} historical",
        f"{location} establishment",
    ]
    return terms

def filter_historical_dates(dates, cutoff_year=2000):
    """Filter dates to prefer historical ones."""
    if not dates:
        return None
    historical_dates = [d for d in dates if d.year < cutoff_year]
    if historical_dates:
        return min(historical_dates)  # Return the earliest historical date
    return dates[0]  # If no historical dates found, return the first date

def get_location_with_context(location, description):
    """Add geographic context to location names."""
    contexts = [location]
    
    # Add USA context
    contexts.append(f"{location}, USA")
    
    # Add state context if found in description
    states = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
              "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", 
              "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", 
              "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", 
              "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", 
              "New Hampshire", "New Jersey", "New Mexico", "New York", 
              "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", 
              "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", 
              "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", 
              "West Virginia", "Wisconsin", "Wyoming"]
    
    for state in states:
        if state in description:
            contexts.append(f"{location}, {state}")
    
    return contexts

def try_wikipedia_sections(page):
    """Try to find dates in specific Wikipedia sections."""
    sections_to_try = ['history', 'background', 'establishment', 'founding', 'early history']
    all_dates = []
    
    # Try specific sections first
    for section in sections_to_try:
        try:
            section_content = page.section(section)
            if section_content:
                section_dates = list(datefinder.find_dates(section_content))
                if section_dates:
                    all_dates.extend(section_dates)
        except:
            continue
    
    # If no dates found in sections, try full article
    if not all_dates:
        all_dates = list(datefinder.find_dates(page.content))
    
    return all_dates

def extract_date(description):
    """Extracts dates using datefinder, regex, dateparser, and Wikipedia."""
    wikipedia_used = False

    # Preprocessing
    description = re.sub(r"March \d{4} Update|February \d{4} Correction", "", description, flags=re.IGNORECASE)
    description = description.replace("wasn't", "was not").replace("didn't", "did not")

    original_description = description

    # Regex pattern for dates like "YYYY/MM/DD"
    date_pattern_yyyy_mm_dd = re.compile(r"\b(\d{4})[/-](\d{1,2})[/-](\d{1,2})\b")
    match_yyyy_mm_dd = date_pattern_yyyy_mm_dd.search(description)
    if match_yyyy_mm_dd:
        try:
            date_object = datetime.strptime(match_yyyy_mm_dd.group(0), '%Y/%m/%d')
            description = description.replace(match_yyyy_mm_dd.group(0), "") #Remove matched text
            return date_object.strftime('%Y/%m/%d'), wikipedia_used
        except ValueError:
            pass

    # Regex pattern for dates like "January 1, 2023"
    date_pattern = re.compile(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})\b", re.IGNORECASE)
    match = date_pattern.search(description)
    if match:
        try:
            date_object = datetime.strptime(match.group(0), '%B %d, %Y')
            description = description.replace(match.group(0), "") #Remove matched text
            return date_object.strftime('%Y/%m/%d'), wikipedia_used
        except ValueError:
            pass  # Handle invalid dates

    # Regex pattern for dates like "Jan 2023"
    date_pattern_month_year = re.compile(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*[,]?\s*(\d{4})\b", re.IGNORECASE)
    match_month_year = date_pattern_month_year.search(description)
    if match_month_year:
        try:
            date_string = match_month_year.group(0)
            date_object = datetime.strptime(date_string, '%B %Y') if len(date_string.split()) == 2 else datetime.strptime(date_string, '%b %Y')
            description = description.replace(match_month_year.group(0), "") #Remove matched text
            return date_object.strftime('%Y/%m/%d'), wikipedia_used
        except ValueError:
            pass

    # Regex pattern for dates like "1/1/2023" or "1-1-2023"
    date_pattern_numeric = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b")
    match_numeric = date_pattern_numeric.search(description)
    if match_numeric:
        try:
            date_object = datetime.strptime(match_numeric.group(0), '%m/%d/%Y') #Assuming MM/DD/YYYY
            description = description.replace(match_numeric.group(0), "") #Remove matched text
            return date_object.strftime('%Y/%m/%d'), wikipedia_used
        except ValueError:
            try:
                date_object = datetime.strptime(match_numeric.group(0), '%d/%m/%Y') #Trying DD/MM/YYYY if first fails
                description = description.replace(match_numeric.group(0), "") #Remove matched text
                return date_object.strftime('%Y/%m/%d'), wikipedia_used
            except:
                pass

    # Regex pattern for dates like "January 1st, 2023"
    date_pattern_ordinal = re.compile(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2})(?:st|nd|rd|th),?\s*(\d{4})\b", re.IGNORECASE)
    match_ordinal = date_pattern_ordinal.search(description)
    if match_ordinal:
        try:
            date_string = match_ordinal.group(0)
            date_object = datetime.strptime(date_string, '%B %d, %Y')
            description = description.replace(match_ordinal.group(0), "") #Remove matched text
            return date_object.strftime('%Y/%m/%d'), wikipedia_used
        except ValueError:
            pass

    # Regex pattern for dates like "1.1.2023"
    date_pattern_numeric_dots = re.compile(r"\b(\d{1,2})[.](\d{1,2})[.](\d{4})\b")
    match_numeric_dots = date_pattern_numeric_dots.search(description)
    if match_numeric_dots:
        try:
            date_object = datetime.strptime(match_numeric_dots.group(0), '%m.%d.%Y') #Assuming MM.DD.YYYY
            description = description.replace(match_numeric_dots.group(0), "") #Remove matched text
            return date_object.strftime('%Y/%m/%d'), wikipedia_used
        except ValueError:
            try:
                date_object = datetime.strptime(match_numeric_dots.group(0), '%d.%m.%Y') #Trying DD.MM.YYYY if first fails
                description = description.replace(match_numeric_dots.group(0), "") #Remove matched text
                return date_object.strftime('%Y/%m/%d'), wikipedia_used
            except:
                pass

    # Regex pattern for years with "around" or "about"
    date_pattern_approx = re.compile(r"\b(around|about)\s*(\d{4})\b", re.IGNORECASE)
    match_approx = date_pattern_approx.search(description)
    if match_approx:
        try:
            year = int(match_approx.group(2))
            description = description.replace(match_approx.group(0), "") #Remove matched text
            return f"{year}/01/01", wikipedia_used
        except ValueError:
            pass

    # Handle "Early," "Mid," and "Late"
    decade_pattern = re.compile(r"(Early|Mid|Late) (\d{2})s", re.IGNORECASE)
    decade_match = decade_pattern.search(description)
    if decade_match:
        prefix = decade_match.group(1)
        decade = int(decade_match.group(2))
        year = 1900 + decade  # Default to 20th century
        if decade < 25: #If less than 25, assume 2000s
            year = 2000 + decade
        if prefix.lower() == "early":
            month = "02"
        elif prefix.lower() == "mid":
            month = "06"
        elif prefix.lower() == "late":
            month = "10"
        else:
            month = "01"
        description = description.replace(decade_match.group(0), "") #Remove matched text
        return f"{year}/{month}/01", wikipedia_used

    #Handle "Since" keyword
    since_pattern = re.compile(r"since (\d{4})", re.IGNORECASE)
    since_match = since_pattern.search(description)
    if since_match:
        year = int(since_match.group(1))
        description = description.replace(since_match.group(0), "") #Remove matched text
        return f"{year}/01/01", wikipedia_used

   #Capture Year Ranges
    year_range_pattern = re.compile(r"(\d{4})s?[ -]+(\d{4})s?", re.IGNORECASE)
    year_range_match = year_range_pattern.search(description)
    if year_range_match:
        year1 = int(year_range_match.group(1))
        year2 = int(year_range_match.group(2))
        description = description.replace(year_range_match.group(0), "") #Remove matched text
        return f"{year1}/01/01", wikipedia_used

    #More General Year Regex
    year_pattern = re.compile(r"\b(18|19|20)\d{2}\b", re.IGNORECASE)  #Catches 18xx, 19xx, 20xx
    year_match = year_pattern.search(description)
    if year_match:
        year = int(year_match.group(0))
        description = description.replace(year_match.group(0), "") #Remove matched text
        return f"{year}/01/01", wikipedia_used
    
    # Add a regex pattern for just the year
    year_only_pattern = re.compile(r"^(18|19|20)\d{2}$")
    year_only_match = year_only_pattern.search(description)
    if year_only_match:
        year = int(year_only_match.group(0))
        description = description.replace(year_only_match.group(0), "")
        return f"{year}/01/01", wikipedia_used

    # Named Entity Recognition
    doc = nlp(description)
    locations = [ent.text for ent in doc.ents if ent.label_ == "GPE"]

    # Enhanced Wikipedia Lookup
    for location in locations:
        locations_with_context = get_location_with_context(location, description)
        
        for loc in locations_with_context:
            for search_term in generate_search_terms(loc):
                try:
                    # Check cache first
                    if search_term in wikipedia_cache:
                        page_content = wikipedia_cache[search_term]
                    else:
                        print(f"Trying Wikipedia search term: {search_term}")
                        page = wikipedia.page(search_term, auto_suggest=True)
                        page_content = page.content
                        wikipedia_cache[search_term] = page_content  # Store in cache
                    
                    # Try to find dates in specific sections and full article
                    wiki_dates = try_wikipedia_sections(page)
                    
                    if wiki_dates:
                        # Filter for historical dates
                        best_date = filter_historical_dates(wiki_dates)
                        if best_date:
                            print(f"Wikipedia date found: {best_date}")
                            wikipedia_used = True
                            return best_date.strftime('%Y/%m/%d'), wikipedia_used
                            
                except wikipedia.exceptions.DisambiguationError as e:
                    print(f"Disambiguation error for {search_term}: {e}")
                    try:
                        # Try the first option that contains the original search term
                        options = [opt for opt in e.options if search_term.lower() in opt.lower()]
                        if options:
                            page = wikipedia.page(options[0], auto_suggest=False)
                            wiki_dates = try_wikipedia_sections(page)
                            if wiki_dates:
                                best_date = filter_historical_dates(wiki_dates)
                                if best_date:
                                    print(f"Wikipedia date found from disambiguation: {best_date}")
                                    wikipedia_used = True
                                    return best_date.strftime('%Y/%m/%d'), wikipedia_used
                    except:
                        continue
                
                except wikipedia.exceptions.PageError:
                    print(f"Page not found for {search_term}")
                    continue
                    
                except Exception as e:
                    print(f"Error during Wikipedia lookup: {e}")
                    continue
                
                # Add delay to avoid rate limiting
                time.sleep(0.1)

    # Use datefinder as a last resort
    dates = list(datefinder.find_dates(description))
    if dates:
        return dates[0].strftime('%Y/%m/%d'), wikipedia_used

    # Use dateparser as a last resort
    try:
        date_object = dateparser.parse(original_description)
        if date_object:
            return date_object.strftime('%Y/%m/%d'), wikipedia_used
    except:
        pass

    # If all else fails
    return None, wikipedia_used

def determine_haunted_date(df):
    default_date_count = 0
    correct_date_count = 0
    dates = []
    wikipedia_date_count = 0

    # Limit to the first 12000 rows
    for i, description in enumerate(df['description']):
        if i >= 12000:
            break
        date_found, wikipedia_used = extract_date(description)
        if date_found:
            dates.append(date_found)
            correct_date_count += 1
            if wikipedia_used:
                wikipedia_date_count += 1
        else:
            dates.append('2025/01/01')
            default_date_count += 1

    df['haunted_places_date'] = dates

    print(f"Number of default dates set: {default_date_count}")
    print(f"Number of dates correctly found: {correct_date_count}")
    print(f"Number of dates found using Wikipedia: {wikipedia_date_count}")
    return df

if __name__ == "__main__":
    tsv_file_path = '../Datasets/haunted_places.tsv'
    df = pd.read_csv(tsv_file_path, sep='\t', nrows=12000)  # Read only the first 12000 rows
    df_with_dates = determine_haunted_date(df)
    output_file_path = '../Datasets/haunted_places_dates_wiki_2500.tsv'
    df_with_dates.to_csv(output_file_path, sep='\t', index=False)
    print(f"DataFrame with dates saved to: {output_file_path}")