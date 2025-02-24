import re
import datefinder
import pandas as pd
from datetime import datetime
import dateparser  # Import dateparser

def extract_date(description):
    """Extracts dates using datefinder, regex, and dateparser."""

    # Preprocessing
    description = re.sub(r"March \d{4} Update|February \d{4} Correction", "", description, flags=re.IGNORECASE)  # Remove update phrases, case-insensitive
    description = description.replace("wasn't", "was not").replace("didn't", "did not") #Expand contractions

    original_description = description #Keep a copy for dateparser

    # Regex pattern for dates like "YYYY/MM/DD"
    date_pattern_yyyy_mm_dd = re.compile(r"\b(\d{4})[/-](\d{1,2})[/-](\d{1,2})\b")
    match_yyyy_mm_dd = date_pattern_yyyy_mm_dd.search(description)
    if match_yyyy_mm_dd:
        try:
            date_object = datetime.strptime(match_yyyy_mm_dd.group(0), '%Y/%m/%d')
            description = description.replace(match_yyyy_mm_dd.group(0), "") #Remove matched text
            return date_object.strftime('%Y/%m/%d')
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
            return date_object.strftime('%Y/%m/%d')
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
            return date_object.strftime('%Y/%m/%d')
        except ValueError:
            pass

    # Regex pattern for dates like "1/1/2023" or "1-1-2023"
    date_pattern_numeric = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b")
    match_numeric = date_pattern_numeric.search(description)
    if match_numeric:
        try:
            date_object = datetime.strptime(match_numeric.group(0), '%m/%d/%Y') #Assuming MM/DD/YYYY
            description = description.replace(match_numeric.group(0), "") #Remove matched text
            return date_object.strftime('%Y/%m/%d')
        except ValueError:
            try:
                date_object = datetime.strptime(match_numeric.group(0), '%d/%m/%Y') #Trying DD/MM/YYYY if first fails
                description = description.replace(match_numeric.group(0), "") #Remove matched text
                return date_object.strftime('%Y/%m/%d')
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
            return date_object.strftime('%Y/%m/%d')
        except ValueError:
            pass

    # Regex pattern for dates like "1.1.2023"
    date_pattern_numeric_dots = re.compile(r"\b(\d{1,2})[.](\d{1,2})[.](\d{4})\b")
    match_numeric_dots = date_pattern_numeric_dots.search(description)
    if match_numeric_dots:
        try:
            date_object = datetime.strptime(match_numeric_dots.group(0), '%m.%d.%Y') #Assuming MM.DD.YYYY
            description = description.replace(match_numeric_dots.group(0), "") #Remove matched text
            return date_object.strftime('%Y/%m/%d')
        except ValueError:
            try:
                date_object = datetime.strptime(match_numeric_dots.group(0), '%d.%m.%Y') #Trying DD.MM.YYYY if first fails
                description = description.replace(match_numeric_dots.group(0), "") #Remove matched text
                return date_object.strftime('%Y/%m/%d')
            except:
                pass

    # Regex pattern for years with "around" or "about"
    date_pattern_approx = re.compile(r"\b(around|about)\s*(\d{4})\b", re.IGNORECASE)
    match_approx = date_pattern_approx.search(description)
    if match_approx:
        try:
            year = int(match_approx.group(2))
            description = description.replace(match_approx.group(0), "") #Remove matched text
            return f"{year}/01/01"
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
        return f"{year}/{month}/01"

    #Handle "Since" keyword
    since_pattern = re.compile(r"since (\d{4})", re.IGNORECASE)
    since_match = since_pattern.search(description)
    if since_match:
        year = int(since_match.group(1))
        description = description.replace(since_match.group(0), "") #Remove matched text
        return f"{year}/01/01"

   #Capture Year Ranges
    year_range_pattern = re.compile(r"(\d{4})s?[ -]+(\d{4})s?", re.IGNORECASE)
    year_range_match = year_range_pattern.search(description)
    if year_range_match:
        year1 = int(year_range_match.group(1))
        year2 = int(year_range_match.group(2))
        description = description.replace(year_range_match.group(0), "") #Remove matched text
        return f"{year1}/01/01"

    #More General Year Regex
    year_pattern = re.compile(r"\b(18|19|20)\d{2}\b", re.IGNORECASE)  #Catches 18xx, 19xx, 20xx
    year_match = year_pattern.search(description)
    if year_match:
        year = int(year_match.group(0))
        description = description.replace(year_match.group(0), "") #Remove matched text
        return f"{year}/01/01"

    # Use datefinder as a last resort
    dates = list(datefinder.find_dates(description))
    if dates:
        return dates[0].strftime('%Y/%m/%d')

    # Use dateparser as a last resort
    try:
        date_object = dateparser.parse(original_description) #Use original description
        if date_object:
            return date_object.strftime('%Y/%m/%d')
    except:
        pass

    # If all else fails
    return None


def determine_haunted_date(tsv_file):
    df = pd.read_csv(tsv_file, sep='\t')
    default_date_count = 0
    correct_date_count = 0
    dates = []

    for description in df['description']:
        date_found = extract_date(description)
        if date_found:
            dates.append(date_found)
            correct_date_count += 1
        else:
            dates.append('2025/01/01')
            default_date_count += 1

    df['haunted_places_date'] = dates
    print(f"Number of default dates set: {default_date_count}")
    print(f"Number of dates correctly found: {correct_date_count}")
    return df


if __name__ == "__main__":
    tsv_file_path = '/Users/jfulch/git/school/dsci-550/dsci-550-hw1-haunted-places/Datasets/haunted_places.tsv'
    df_with_dates = determine_haunted_date(tsv_file_path)
    output_file_path = '/Users/jfulch/git/school/dsci-550/dsci-550-hw1-haunted-places/Datasets/haunted_places_dates.tsv'
    df_with_dates.to_csv(output_file_path, sep='\t', index=False)
    print(f"DataFrame with dates saved to: {output_file_path}")