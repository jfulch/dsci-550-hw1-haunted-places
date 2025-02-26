import re
import os
import json
import pickle
import hashlib
import datefinder
import pandas as pd
from datetime import datetime
import dateparser
import wikipedia
import time
import requests
from bs4 import BeautifulSoup
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from googlesearch import search as GoogleSearch
import logging
from collections import Counter
from functools import lru_cache

# Note: To reset or clear the cache, set the reset parameter to True at the bottom of the main script. 

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Cache directories
CACHE_DIR = os.path.join(os.getcwd(), 'cache')
WIKIPEDIA_CACHE_FILE = os.path.join(CACHE_DIR, 'wikipedia_cache.pkl')
SEARCH_CACHE_FILE = os.path.join(CACHE_DIR, 'search_cache.pkl')
WEB_CACHE_FILE = os.path.join(CACHE_DIR, 'web_cache.pkl')
RESULTS_CACHE_FILE = os.path.join(CACHE_DIR, 'results_cache.pkl')
PROGRESS_FILE = os.path.join(CACHE_DIR, 'progress.json')

# Create cache directory if it doesn't exist
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# Initialize caches
wikipedia_cache = {}
search_cache = {}
web_cache = {}
results_cache = {}
location_date_cache = {}
processed_ids = set()

# Load existing caches if available
def load_cache():
    global wikipedia_cache, search_cache, web_cache, results_cache, processed_ids
    
    try:
        if os.path.exists(WIKIPEDIA_CACHE_FILE):
            with open(WIKIPEDIA_CACHE_FILE, 'rb') as f:
                wikipedia_cache = pickle.load(f)
            logger.info(f"Loaded {len(wikipedia_cache)} Wikipedia cache entries")
    except Exception as e:
        logger.error(f"Error loading Wikipedia cache: {e}")
    
    try:
        if os.path.exists(SEARCH_CACHE_FILE):
            with open(SEARCH_CACHE_FILE, 'rb') as f:
                search_cache = pickle.load(f)
            logger.info(f"Loaded {len(search_cache)} search cache entries")
    except Exception as e:
        logger.error(f"Error loading search cache: {e}")
    
    try:
        if os.path.exists(WEB_CACHE_FILE):
            with open(WEB_CACHE_FILE, 'rb') as f:
                web_cache = pickle.load(f)
            logger.info(f"Loaded {len(web_cache)} web cache entries")
    except Exception as e:
        logger.error(f"Error loading web cache: {e}")
    
    try:
        if os.path.exists(RESULTS_CACHE_FILE):
            with open(RESULTS_CACHE_FILE, 'rb') as f:
                results_cache = pickle.load(f)
            logger.info(f"Loaded {len(results_cache)} results cache entries")
    except Exception as e:
        logger.error(f"Error loading results cache: {e}")
    
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                progress_data = json.load(f)
                processed_ids = set(progress_data.get('processed_ids', []))
            logger.info(f"Loaded progress data with {len(processed_ids)} processed IDs")
    except Exception as e:
        logger.error(f"Error loading progress data: {e}")

# Save caches periodically
def save_cache(force=False):
    global wikipedia_cache, search_cache, web_cache, results_cache, processed_ids
    
    # Only save every 100 new entries unless forced
    wiki_cache_size = len(wikipedia_cache)
    if force or wiki_cache_size % 100 == 0:
        try:
            with open(WIKIPEDIA_CACHE_FILE, 'wb') as f:
                pickle.dump(wikipedia_cache, f)
            logger.info(f"Saved {wiki_cache_size} Wikipedia cache entries")
        except Exception as e:
            logger.error(f"Error saving Wikipedia cache: {e}")
    
    search_cache_size = len(search_cache)
    if force or search_cache_size % 100 == 0:
        try:
            with open(SEARCH_CACHE_FILE, 'wb') as f:
                pickle.dump(search_cache, f)
            logger.info(f"Saved {search_cache_size} search cache entries")
        except Exception as e:
            logger.error(f"Error saving search cache: {e}")
    
    web_cache_size = len(web_cache)
    if force or web_cache_size % 100 == 0:
        try:
            with open(WEB_CACHE_FILE, 'wb') as f:
                pickle.dump(web_cache, f)
            logger.info(f"Saved {web_cache_size} web cache entries")
        except Exception as e:
            logger.error(f"Error saving web cache: {e}")
    
    results_cache_size = len(results_cache)
    if force or results_cache_size % 100 == 0:
        try:
            with open(RESULTS_CACHE_FILE, 'wb') as f:
                pickle.dump(results_cache, f)
            logger.info(f"Saved {results_cache_size} results cache entries")
        except Exception as e:
            logger.error(f"Error saving results cache: {e}")
    
    # Always save progress
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump({'processed_ids': list(processed_ids)}, f)
        logger.info(f"Saved progress with {len(processed_ids)} processed IDs")
    except Exception as e:
        logger.error(f"Error saving progress: {e}")

# User agents to rotate for web requests
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
]

def get_random_user_agent():
    """Return a random user agent to avoid detection."""
    return random.choice(USER_AGENTS)

def get_cache_key(text):
    """Generate a stable cache key for any text."""
    return hashlib.md5(text.encode()).hexdigest()

@lru_cache(maxsize=1000)
def generate_search_terms(location):
    """Generate various search terms for a location, prioritizing most likely to succeed."""
    # Primary terms - most likely to succeed
    primary_terms = [
        f"{location} history date",
        f"{location} haunted history"
    ]
    
    # Secondary terms - try if primary fails
    secondary_terms = [
        f"{location} when built",
        f"{location} founded",
        f"{location} established",
        f"history of {location}"
    ]
    
    return primary_terms + secondary_terms
def filter_historical_dates(dates, cutoff_year=2000):
    """Filter dates to prefer historical ones."""
    if not dates:
        return None
    
    current_year = datetime.now().year
    
    # Filter out future dates and very recent dates
    valid_dates = [d for d in dates if d.year <= current_year and d.year > 1500]
    
    # First, try to find dates before cutoff year
    historical_dates = [d for d in valid_dates if d.year < cutoff_year]
    
    if historical_dates:
        # Return the earliest historical date, prioritizing older dates
        return min(historical_dates, key=lambda d: d.year)  
    elif valid_dates:
        # If no historical dates found, return the earliest date
        return min(valid_dates, key=lambda d: d.year)
    
    return None

def get_location_with_context(location, description):
    """Add geographic context to location names, prioritized by likelihood."""
    contexts = []
    
    # Primary context - most likely to succeed
    contexts.append(location)
    
    # Check for states in the description
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
    
    # Check if any states are mentioned
    for state in states:
        if state in description:
            # Add state context as higher priority
            contexts.insert(0, f"{location}, {state}")
            break
    
    # Add USA context as a fallback
    contexts.append(f"{location}, USA")
    
    # Add 'haunted' context last as it's more specific
    contexts.append(f"{location} haunted")
    
    return contexts

def try_wikipedia_sections(page):
    """Try to find dates in specific Wikipedia sections, prioritizing most relevant."""
    # Prioritized sections for efficiency
    priority_sections = ['history', 'founding', 'establishment']
    secondary_sections = ['background', 'early history', 'construction', 'origins']
    
    all_dates = []
    
    # Try priority sections first
    for section in priority_sections:
        try:
            section_content = page.section(section)
            if section_content:
                section_dates = list(datefinder.find_dates(section_content))
                if section_dates:
                    all_dates.extend(section_dates)
                    # Early return if we found dates in high-priority sections
                    if len(all_dates) > 0:
                        return all_dates
        except:
            continue
    
    # If no dates in priority sections, try secondary sections
    if not all_dates:
        for section in secondary_sections:
            try:
                section_content = page.section(section)
                if section_content:
                    section_dates = list(datefinder.find_dates(section_content))
                    if section_dates:
                        all_dates.extend(section_dates)
            except:
                continue
    
    # If still no dates, try the first paragraph which often has founding info
    if not all_dates:
        try:
            # Get the first paragraph of content
            paragraphs = page.content.split('\n\n')
            if paragraphs:
                first_paragraph = paragraphs[0]
                para_dates = list(datefinder.find_dates(first_paragraph))
                if para_dates:
                    all_dates.extend(para_dates)
        except:
            pass
    
    # As a last resort, try the full article
    if not all_dates:
        try:
            all_dates = list(datefinder.find_dates(page.content[:5000]))  # Just scan first 5000 chars for efficiency
        except:
            pass
    
    return all_dates

def search_wikipedia(search_term):
    """Search Wikipedia for a single term with caching."""
    cache_key = get_cache_key(f"wikipedia_search_{search_term}")
    
    # Check cache
    if cache_key in wikipedia_cache:
        return wikipedia_cache[cache_key]
    
    try:
        # Add a slight delay to avoid being blocked
        time.sleep(random.uniform(0.2, 0.5))
        
        # Search Wikipedia
        search_results = wikipedia.search(search_term, results=1)
        
        # Cache the results
        wikipedia_cache[cache_key] = search_results
        return search_results
    except Exception as e:
        logger.error(f"Wikipedia search error for '{search_term}': {e}")
        wikipedia_cache[cache_key] = []
        return []

def get_wikipedia_page(title):
    """Get a Wikipedia page with caching."""
    cache_key = get_cache_key(f"wikipedia_page_{title}")
    
    # Check cache
    if cache_key in wikipedia_cache:
        return wikipedia_cache[cache_key]
    
    try:
        # Add a slight delay to avoid being blocked
        time.sleep(random.uniform(0.2, 0.5))
        
        # Get Wikipedia page
        page = wikipedia.page(title)
        
        # Cache the page
        wikipedia_cache[cache_key] = page
        return page
    except Exception as e:
        logger.error(f"Wikipedia page error for '{title}': {e}")
        wikipedia_cache[cache_key] = None
        return None

def search_google(query, num_results=3):
    """Search Google for information about a location, with limited results for efficiency."""
    cache_key = get_cache_key(f"google_search_{query}")
    
    # Check cache
    if cache_key in search_cache:
        return search_cache[cache_key]
    
    urls = []
    try:
        # Add a delay to avoid being blocked
        time.sleep(random.uniform(1.0, 2.0))
        
        # Perform the search with GoogleSearch class
        gs = GoogleSearch()
        search_results = gs.search(query, num_results=num_results)
        
        # Process the search results
        count = 0
        for result in search_results:
            url = result.url  # Get URL from the result object
            parsed_url = urlparse(url)
            
            # Skip certain domains that might not be relevant
            if any(domain in parsed_url.netloc for domain in [
                'youtube.com', 'facebook.com', 'twitter.com', 'instagram.com',
                'pinterest.com', 'amazon.com', 'ebay.com'
            ]):
                continue
                
            # Prioritize historical and educational sites
            if any(domain in parsed_url.netloc for domain in [
                'nps.gov', 'history.com', 'britannica.com', 'si.edu', 
                'loc.gov', 'archives.gov', 'museums', 'historical'
            ]):
                # Put these at the beginning
                urls.insert(0, url)
            else:
                urls.append(url)
            
            count += 1
            if count >= num_results:
                break
        
        # Cache the results
        search_cache[cache_key] = urls
        return urls
    except Exception as e:
        logger.error(f"Google search error for '{query}': {e}")
        search_cache[cache_key] = []
        return []

def extract_date_from_web_page(url):
    """Extract date information from a web page."""
    cache_key = get_cache_key(f"web_page_{url}")
    
    # Check cache
    if cache_key in web_cache:
        return web_cache[cache_key]
    
    try:
        # Add a delay to avoid being blocked
        time.sleep(random.uniform(1.0, 2.0))
        
        headers = {'User-Agent': get_random_user_agent()}
        
        # Set a strict timeout
        response = requests.get(url, headers=headers, timeout=8)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # First try focused extraction from history-related elements
            potential_elements = []
            
            # Look for elements with relevant words in class or id
            for element in soup.find_all(['div', 'p', 'section', 'article']):
                class_str = ' '.join(element.get('class', [])).lower() if element.get('class') else ''
                id_str = element.get('id', '').lower()
                
                if any(word in class_str or word in id_str for word in ['history', 'about', 'info', 'description', 'background']):
                    potential_elements.append(element.get_text())
            
            # Look for paragraphs with date-related keywords
            date_keywords = ['built', 'founded', 'established', 'constructed', 'opened', 'began', 'originated', 'started']
            for p in soup.find_all('p'):
                text = p.get_text().lower()
                if any(keyword in text for keyword in date_keywords):
                    potential_elements.append(p.get_text())
            
            # If no specific elements found, try the meta description and title
            if not potential_elements:
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                if meta_desc:
                    potential_elements.append(meta_desc.get('content', ''))
                
                title = soup.find('title')
                if title:
                    potential_elements.append(title.get_text())
            
            # If still nothing, use just the first few paragraphs (more efficient)
            if not potential_elements:
                paragraphs = soup.find_all('p')[:5]  # Just first 5 paragraphs
                potential_elements = [p.get_text() for p in paragraphs]
            
            # Try to find dates in the elements
            all_dates = []
            for text in potential_elements:
                try:
                    # First look for year patterns which are most reliable
                    year_patterns = [
                        r'\b(in|from|since|established|built|founded|begun|started|dates? back to)(?:\s+in|\s+during)?\s+(\d{4})\b',
                        r'\bbuilt\s+in\s+(\d{4})\b',
                        r'\bfounded\s+in\s+(\d{4})\b',
                        r'\bestablished\s+in\s+(\d{4})\b',
                        r'\b(circa|c\.)?\s*(\d{4})\b'
                    ]
                    
                    for pattern in year_patterns:
                        matches = re.finditer(pattern, text, re.IGNORECASE)
                        for match in matches:
                            try:
                                # Extract the year from the appropriate group
                                year_str = match.group(2) if len(match.groups()) > 1 else match.group(1)
                                year = int(year_str)
                                if 1500 < year < datetime.now().year:  # Reasonable year range
                                    all_dates.append(datetime(year, 1, 1))
                            except (ValueError, IndexError):
                                continue
                    
                    # If no years found, try full date extraction
                    if not all_dates:
                        found_dates = list(datefinder.find_dates(text))
                        all_dates.extend(found_dates)
                except Exception as e:
                    logger.debug(f"Date parsing error: {e}")
                    continue
            
            if all_dates:
                filtered_date = filter_historical_dates(all_dates)
                # Cache the result
                web_cache[cache_key] = filtered_date
                return filtered_date
            
            # Cache null result
            web_cache[cache_key] = None
                
    except Exception as e:
        logger.error(f"Error extracting date from {url}: {e}")
        web_cache[cache_key] = None  # Cache null results to avoid repeated errors
    
    return None

def extract_locations_from_text(text):
    """Extract potential location names from text without using spaCy, with enhanced prioritization."""
    # First sentences often mention the location name
    sentences = re.split(r'[.!?]', text)
    first_sentence = sentences[0] if sentences else text
    
    # Look for capitalized multi-word phrases
    all_locations = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', text)
    first_sentence_locations = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', first_sentence)
    
    # Filter out common non-location capitalized phrases
    common_words = ['I', 'The', 'A', 'An', 'In', 'On', 'At', 'By', 'For', 'With', 
                   'To', 'From', 'And', 'But', 'Or', 'Not', 'He', 'She', 'It', 
                   'They', 'We', 'You', 'Who', 'What', 'Where', 'When', 'Why', 'How']
    
    filtered_locations = [loc for loc in all_locations if loc not in common_words]
    
    # Add locations that appear after "the" (e.g., "the Winchester Mystery House")
    the_locations = re.findall(r'\bthe\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', text, re.IGNORECASE)
    
    # Look for location names in quotes
    quoted_locations = re.findall(r'["\'](.*?)["\']', text)
    quoted_filtered = []
    for quoted in quoted_locations:
        if any(char.isupper() for char in quoted) and len(quoted.split()) <= 5:
            quoted_filtered.append(quoted)
    
    # Build a priority list - first sentence locations get highest priority
    priority_locations = []
    
    # Add mentions that appear in the first sentence
    priority_locations.extend(first_sentence_locations)
    
    # Add "the X" mentions which are often building/location names
    priority_locations.extend(the_locations)
    
    # Add quoted names
    priority_locations.extend(quoted_filtered)
    
    # Add any other capitalized phrases from the text
    for loc in filtered_locations:
        if loc not in priority_locations:
            priority_locations.append(loc)
    
    # Count occurrences and prioritize by frequency
    location_counter = Counter(all_locations)
    
    # Sort the priority list by frequency of mention
    priority_locations.sort(key=lambda x: location_counter.get(x, 0), reverse=True)
    
    # Remove duplicates while preserving order
    unique_locations = []
    for loc in priority_locations:
        if loc not in unique_locations and len(loc) > 2: # skip short names
            unique_locations.append(loc)
    
    return unique_locations

def try_wikipedia_for_location(location, context=""):
    """Try to find dates for a location using Wikipedia only, with early termination."""
    # Check location cache first
    cache_key = get_cache_key(f"location_{location}")
    if cache_key in location_date_cache:
        return location_date_cache[cache_key]
        
    # Generate context-aware search terms
    if context:
        contexts = get_location_with_context(location, context)
    else:
        contexts = [location, f"{location}, USA"]
    
    # Try each context 
    for context in contexts[:2]:  # Limit to just top 2 contexts for efficiency
        # Try direct Wikipedia search
        search_results = search_wikipedia(context)
        
        if search_results:
            # Try just the first, most relevant result
            page = get_wikipedia_page(search_results[0])
            
            if page:
                all_dates = try_wikipedia_sections(page)
                if all_dates:
                    filtered_date = filter_historical_dates(all_dates)
                    if filtered_date:
                        date_str = filtered_date.strftime('%Y/%m/%d')
                        # Cache this result for future use
                        location_date_cache[cache_key] = (date_str, "wikipedia")
                        return date_str, "wikipedia"
    
    # This will try a more specific history search term if basic search failed
    for context in contexts[:1]:  
        history_term = f"{context} history"
        search_results = search_wikipedia(history_term)
        
        if search_results:
            page = get_wikipedia_page(search_results[0])
            
            if page:
                all_dates = try_wikipedia_sections(page)
                if all_dates:
                    filtered_date = filter_historical_dates(all_dates)
                    if filtered_date:
                        date_str = filtered_date.strftime('%Y/%m/%d')
                        location_date_cache[cache_key] = (date_str, "wikipedia")
                        return date_str, "wikipedia"
    
    # No date found in Wikipedia
    location_date_cache[cache_key] = (None, None)
    return None, None

def try_google_for_location(location, context=""):
    """Try to find dates for a location using Google Search and web scraping, with limited queries."""
    # Check location cache first
    cache_key = get_cache_key(f"location_google_{location}")
    if cache_key in location_date_cache:
        return location_date_cache[cache_key]
    
    # Generate targeted search queries
    search_queries = [
        f"{location} historical date built",
        f"{location} when founded history"
    ]
    
    # If we have context, add a context-specific query
    if context:
        search_queries.insert(0, f"{location} history {context}")
    
    # Try just 1-2 queries
    all_urls = []
    for query in search_queries[:2]:  # Limit to just 2 queries for efficiency
        urls = search_google(query, num_results=3)  # Limit to top 3 results
        all_urls.extend(urls)
        
        # Early termination if we found some results
        if len(all_urls) >= 3:
            break
    
    # Deduplicate URLs
    all_urls = list(dict.fromkeys(all_urls))[:5]  # Keep only first 5
    
    # Extract dates from web pages (but limit to 3 concurrent requests)
    dates = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_url = {executor.submit(extract_date_from_web_page, url): url for url in all_urls}
        
        for future in as_completed(future_to_url):
            try:
                url = future_to_url[future]
                date = future.result()
                if date:
                    dates.append(date)
                    # Early termination after finding 2 dates (we'll take earliest)
                    if len(dates) >= 2:
                        for remaining_future in future_to_url:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        break
            except Exception as e:
                logger.error(f"Error processing URL: {e}")
                continue
    
    # If dates were found, return the earliest historical one
    if dates:
        filtered_date = filter_historical_dates(dates)
        if filtered_date:
            date_str = filtered_date.strftime('%Y/%m/%d')
            location_date_cache[cache_key] = (date_str, "google")
            return date_str, "google"
    
    # No dates found via Google
    location_date_cache[cache_key] = (None, None)
    return None, None

def extract_date(description, row_id=None):
    """Extract dates using a prioritized, multi-stage approach with early termination."""
    # Check results cache first
    if row_id and row_id in results_cache:
        return results_cache[row_id]
    
    # Preprocessing to clean up the text
    description = re.sub(r"March \d{4} Update|February \d{4} Correction", "", description, flags=re.IGNORECASE)
    description = description.replace("wasn't", "was not").replace("didn't", "did not")
    original_description = description
    
    # Step 1: Check if the date is directly in the description
    # Extract dates directly from the description using regex first
    date_patterns = [
        (r"\b(\d{4})[/-](\d{1,2})[/-](\d{1,2})\b", '%Y/%m/%d'),  # YYYY/MM/DD
        (r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})\b", '%B %d, %Y'),  # Month DD, YYYY
        (r"\b(in|from|since|established|built|founded|begun|started)(?:\s+in|\s+during)?\s+(\d{4})\b", None),  # "built in YYYY"
        (r"\bcirca\s+(\d{4})\b", None),  # "circa YYYY"
        (r"\bc\.\s*(\d{4})\b", None)  # "c. YYYY"
    ]
    
    for pattern, date_format in date_patterns:
        matches = re.finditer(pattern, description, re.IGNORECASE)
        for match in matches:
            try:
                if date_format:
                    date_object = datetime.strptime(match.group(0), date_format)
                else:
                    # For patterns like "built in YYYY"
                    year_str = match.group(2) if len(match.groups()) > 1 else match.group(1) 
                    year = int(year_str)
                    date_object = datetime(year, 1, 1)
                
                # Check if the date is reasonable
                if 1500 < date_object.year < datetime.now().year:
                    date_str = date_object.strftime('%Y/%m/%d')
                    result = (date_str, "description", "high")
                    if row_id:
                        results_cache[row_id] = result
                    return result
            except (ValueError, IndexError):
                continue
    
    # Try dateparser as a fallback for direct extraction
    parsed_date = dateparser.parse(description, languages=['en'])
    if parsed_date and 1500 < parsed_date.year < datetime.now().year:
        date_str = parsed_date.strftime('%Y/%m/%d')
        result = (date_str, "description", "medium")
        if row_id:
            results_cache[row_id] = result
        return result
    
    # Step 2: Extract locations and look them up
    possible_locations = extract_locations_from_text(original_description)
    
    # Early termination if no locations found
    if not possible_locations:
        result = (None, None, "low")
        if row_id:
            results_cache[row_id] = result
        return result
    
    # Process each location, starting with the most promising ones
    for location in possible_locations[:3]:  # Limit to top 3 locations for efficiency
        # Step 2a: Try Wikipedia first (faster and more reliable)
        date_str, source = try_wikipedia_for_location(location, original_description)
        
        if date_str:
            result = (date_str, source, "high")
            if row_id:
                results_cache[row_id] = result
            return result
        
        # Step 2b: If Wikipedia fails, try Google for just the first/primary location
        if location == possible_locations[0]:
            date_str, source = try_google_for_location(location, original_description)
            
            if date_str:
                result = (date_str, source, "medium")
                if row_id:
                    results_cache[row_id] = result
                return result
    
    # set no date was found
    result = (None, None, "low")
    if row_id:
        results_cache[row_id] = result
    return result

def process_batch(batch):
    """Process a batch of entries."""
    results = []
    for _, row in batch.iterrows():
        row_id = row['id']
        
        # Skip if already processed
        if row_id in processed_ids:
            logger.info(f"Skipping already processed ID {row_id}")
            continue
        
        description = row['description']
        
        # Extract date with optimized algorithm
        date_str, source, confidence = extract_date(description, row_id)
        
        # Add to results
        results.append({
            'id': row_id,
            'description': description,
            'extracted_date': date_str,
            'source': source,
            'confidence': confidence
        })
        
        # Mark as processed
        processed_ids.add(row_id)
        
        # Log progress
        status = "Date found" if date_str else "No date found"
        logger.info(f"Processed ID {row_id}: {status} (Source: {source}, Confidence: {confidence})")
        
        # Save progress periodically
        if len(processed_ids) % 50 == 0:
            save_cache()
    
    return results

def process_dataframe_parallel(df, batch_size=10, max_workers=4):
    """Process a DataFrame in parallel batches, with resumable processing."""
    # Initialize result DataFrame
    result_df = pd.DataFrame(columns=['id', 'description', 'extracted_date', 'source', 'confidence'])
    
    # Get unprocessed rows
    unprocessed_df = df[~df['id'].isin(processed_ids)]
    logger.info(f"Processing {len(unprocessed_df)} unprocessed entries out of {len(df)} total entries")
    
    # If all rows are processed, try to load previous results
    if len(unprocessed_df) == 0:
        if os.path.exists(RESULTS_CACHE_FILE):
            try:
                # Convert cached results to DataFrame
                cached_results = []
                for row_id, (date_str, source, confidence) in results_cache.items():
                    original_row = df[df['id'] == row_id]
                    if not original_row.empty:
                        cached_results.append({
                            'id': row_id,
                            'description': original_row['description'].values[0],
                            'extracted_date': date_str,
                            'source': source,
                            'confidence': confidence
                        })
                
                if cached_results:
                    result_df = pd.DataFrame(cached_results)
                    logger.info(f"Loaded all {len(result_df)} entries from cache")
                    return result_df
            except Exception as e:
                logger.error(f"Error loading cached results: {e}")
    
    # Sort by description length 
    unprocessed_df.loc[:, 'desc_len'] = unprocessed_df['description'].str.len()
    unprocessed_df = unprocessed_df.sort_values('desc_len')
    unprocessed_df = unprocessed_df.drop(columns=['desc_len'])
    
    # Split the df into batches
    batches = [unprocessed_df.iloc[i:i+batch_size] for i in range(0, len(unprocessed_df), batch_size)]
    
    # Save the initial state of caches
    save_cache()
    
    # Calculate total batches for progress reporting
    total_batches = len(batches)
    completed_batches = 0
    
    try:
        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_batch, batch): i for i, batch in enumerate(batches)}
            
            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    if batch_results:  # Skip empty results (already processed)
                        batch_df = pd.DataFrame(batch_results)
                        result_df = pd.concat([result_df, batch_df], ignore_index=True)
                    
                    # Update progress
                    completed_batches += 1
                    progress_pct = (completed_batches / total_batches) * 100
                    logger.info(f"Completed batch {completed_batches}/{total_batches} ({progress_pct:.1f}%)")
                    
                    # Save caches every 10 batches
                    if completed_batches % 10 == 0:
                        save_cache(force=True)
                        
                except Exception as e:
                    logger.error(f"Error processing batch {futures[future]}: {e}")
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user. Saving current progress...")
        save_cache(force=True)
    except Exception as e:
        logger.error(f"Error in parallel processing: {e}")
        save_cache(force=True)
    
    # Save final state
    save_cache(force=True)
    
    # Combine with previously processed results
    if len(result_df) < len(df) and results_cache:
        try:
            # Get processed rows not in current result_df
            cached_ids = set(results_cache.keys())
            missing_ids = cached_ids - set(result_df['id'].astype(str))
            
            cached_results = []
            for row_id in missing_ids:
                date_str, source, confidence = results_cache.get(row_id, (None, None, "low"))
                original_row = df[df['id'] == row_id]
                if not original_row.empty:
                    cached_results.append({
                        'id': row_id,
                        'description': original_row['description'].values[0],
                        'extracted_date': date_str,
                        'source': source,
                        'confidence': confidence
                    })
            
            if cached_results:
                cached_df = pd.DataFrame(cached_results)
                result_df = pd.concat([result_df, cached_df], ignore_index=True)
                logger.info(f"Added {len(cached_results)} entries from cache")
        except Exception as e:
            logger.error(f"Error combining with cached results: {e}")
    
    return result_df

def post_process_results(result_df):
    """Perform post-processing on the results."""
    # Count the number of dates found
    dates_found = result_df['extracted_date'].notnull().sum()
    total_rows = len(result_df)
    
    # Calculate the percentage of dates found
    percent_found = (dates_found / total_rows) * 100 if total_rows > 0 else 0
    
    logger.info(f"Processing summary:")
    logger.info(f"Total entries: {total_rows}")
    logger.info(f"Dates found: {dates_found} ({percent_found:.2f}%)")
    
    # Count by confidence level
    for level in ['high', 'medium', 'low']:
        count = len(result_df[result_df['confidence'] == level])
        logger.info(f"{level.capitalize()} confidence: {count} ({(count/total_rows)*100:.2f}%)")
    
    # Count by source
    for source in ['description', 'wikipedia', 'google', None]:
        source_label = source if source else 'not found'
        count = len(result_df[result_df['source'] == source])
        logger.info(f"Source {source_label}: {count} ({(count/total_rows)*100:.2f}%)")
    
    # Most common years
    if dates_found > 0:
        # Extract years from dates
        result_df['year'] = result_df['extracted_date'].str.slice(0, 4)
        year_counts = result_df['year'].value_counts().head(10)
        logger.info(f"Most common years: {dict(year_counts)}")

    # Set default date for missing entries
    if dates_found < total_rows:
        # Fill missing dates with default value
        result_df.loc[result_df['extracted_date'].isnull(), 'extracted_date'] = '2025/01/01'
        result_df.loc[result_df['extracted_date'] == '2025/01/01', 'source'] = 'default'
        result_df.loc[result_df['extracted_date'] == '2025/01/01', 'confidence'] = 'low'
        logger.info(f"Added default date (2025/01/01) to {total_rows - dates_found} entries")
        
        return result_df

# Define a function to clean up and prepare the dataset
def prepare_dataset(df):
    """Clean and prepare the dataset for processing."""
    # Ensure ID column exists
    if 'id' not in df.columns:
        df['id'] = df.index.astype(str)
    
    # Ensure description column exists
    if 'description' not in df.columns:
        # Look for other potential text columns
        text_columns = [col for col in df.columns if df[col].dtype == 'object']
        if text_columns:
            # Use the column with the longest text on average
            avg_lengths = {col: df[col].astype(str).str.len().mean() for col in text_columns}
            best_column = max(avg_lengths.items(), key=lambda x: x[1])[0]
            df['description'] = df[best_column]
            logger.info(f"Using '{best_column}' as the description column")
        else:
            raise ValueError("No suitable text column found for descriptions")
    
    # Convert IDs to strings for consistent handling
    df['id'] = df['id'].astype(str)
    
    # Remove any duplicate IDs
    before_count = len(df)
    df = df.drop_duplicates(subset=['id'])
    after_count = len(df)
    if before_count > after_count:
        logger.info(f"Removed {before_count - after_count} duplicate IDs")
    
    return df

def create_result_df(df):
    """Create a result DataFrame from cache data."""
    results = []
    for _, row in df.iterrows():
        str_id = str(row['id'])
        if str_id in results_cache:
            date_str, source, confidence = results_cache[str_id]
            results.append({
                'id': row['id'],
                'description': row['description'],
                'extracted_date': date_str,
                'source': source,
                'confidence': confidence
            })
    
    return pd.DataFrame(results)

def merge_with_original(original_df, result_df):
    """Merge the extracted dates back into the original dataset."""
    # Create a copy of the original df
    merged_df = original_df.copy()
    
    # Convert IDs to numeric if possible for proper matching
    try:
        result_df['id_numeric'] = pd.to_numeric(result_df['id'])
        # If successful, this means IDs are likely row indices
        
        # Checking if merged_df has an index that matches
        if len(merged_df) == result_df['id_numeric'].max() + 1:
            
            # Add extracted info based on index
            for idx, row in result_df.iterrows():
                try:
                    id_num = int(row['id'])
                    if 0 <= id_num < len(merged_df):
                        merged_df.loc[id_num, 'evidence_date'] = row['extracted_date']
                        merged_df.loc[id_num, 'date_source'] = row['source']
                        merged_df.loc[id_num, 'date_confidence'] = row['confidence']
                except (ValueError, TypeError):
                    pass
        else:
            # Create a composite key from location+city for matching
            merged_df['temp_key'] = merged_df['location'] + '|' + merged_df['city']
            result_df['temp_key'] = result_df['description'].apply(
                lambda x: x.split(',')[0] + '|' + (x.split(',')[1].strip() if len(x.split(',')) > 1 else '')
                if isinstance(x, str) and ',' in x else '')
            
            # Now join on this composite key
            key_dict = dict(zip(result_df['temp_key'], result_df['extracted_date']))
            source_dict = dict(zip(result_df['temp_key'], result_df['source']))
            conf_dict = dict(zip(result_df['temp_key'], result_df['confidence']))
            
            merged_df['evidence_date'] = merged_df['temp_key'].map(key_dict)
            merged_df['date_source'] = merged_df['temp_key'].map(source_dict)
            merged_df['date_confidence'] = merged_df['temp_key'].map(conf_dict)
            
            # Drop the temporary key
            merged_df = merged_df.drop(columns=['temp_key'])
            
    except (ValueError, TypeError):
        # Just directly map using the id strings
        merged_df['id'] = merged_df.index.astype(str)
        
        date_dict = dict(zip(result_df['id'], result_df['extracted_date']))
        source_dict = dict(zip(result_df['id'], result_df['source']))
        confidence_dict = dict(zip(result_df['id'], result_df['confidence']))
        
        merged_df['evidence_date'] = merged_df['id'].map(date_dict)
        merged_df['date_source'] = merged_df['id'].map(source_dict)
        merged_df['date_confidence'] = merged_df['id'].map(confidence_dict)
    
    # Fill any missing values
    merged_df['evidence_date'] = merged_df['evidence_date'].fillna('2025/01/01')
    merged_df['date_source'] = merged_df['date_source'].fillna('not_processed')
    merged_df['date_confidence'] = merged_df['date_confidence'].fillna('none')
    
    # Remove the temporary id column if we created one
    if 'id' in merged_df.columns and 'id' not in original_df.columns:
        merged_df = merged_df.drop(columns=['id'])
    
    return merged_df

def main(input_file, output_file, batch_size=10, max_workers=4, resume=True, reset=False, skip_problematic=False):
    """Main function with additional options to handle problematic entries."""

    # Check if reset is requested
    if reset:
        # Delete cache files if they exist
        for cache_file in [WIKIPEDIA_CACHE_FILE, SEARCH_CACHE_FILE, WEB_CACHE_FILE,
                          RESULTS_CACHE_FILE, PROGRESS_FILE]:
            if os.path.exists(cache_file):
                os.remove(cache_file)
                logger.info(f"Reset: Deleted {cache_file}")
        # Reset global caches and processed IDs
        global wikipedia_cache, search_cache, web_cache, results_cache, processed_ids
        wikipedia_cache = {}
        search_cache = {}
        web_cache = {}
        results_cache = {}
        processed_ids = set()

    logger.info(f"Starting optimized date extraction process for {input_file}")

    # Load caches if resuming
    if resume and not reset:
        load_cache()

    # Load the data
    df = pd.read_table(input_file)  # Changed to read_table
    logger.info(f"Loaded {len(df)} entries from {input_file}")

    # Save the original df for later merging
    original_df = df.copy()

    # Prepare the dataset
    df = prepare_dataset(df)

    # Get unprocessed rows to identify potential problem entries
    unprocessed_df = df[~df['id'].isin(processed_ids)]
    unprocessed_ids = unprocessed_df['id'].tolist()

    logger.info(f"Remaining unprocessed IDs: {unprocessed_ids}")

    # If requested, mark problematic entries as processed to skip them
    if skip_problematic and len(unprocessed_ids) < 50:  # applying to small numbers of stuck entries
        for problem_id in unprocessed_ids:
            logger.warning(f"Marking problematic ID {problem_id} as processed with default values")
            processed_ids.add(problem_id)
            results_cache[problem_id] = ('2025/01/01', 'skipped', 'low')

        # Save progress after skipping
        save_cache(force=True)
        logger.info("Problematic entries marked as skipped")

        # Exit early if we've skipped all remaining entries
        if df[~df['id'].isin(processed_ids)].empty:
            # Process results and save
            result_df = create_result_df(df)
            result_df = post_process_results(result_df)
            result_df.to_csv(output_file, sep='\t', index=False)  # Changed to .tsv
            logger.info(f"Results saved to {output_file}")

            # Create merged dataset with dates added to original data
            merged_df = merge_with_original(original_df, result_df)
            merged_output = f"{os.path.splitext(output_file)[0]}_evidence_date.tsv"  # Changed to .tsv
            merged_df.to_csv(merged_output, sep='\t', index=False)  # Changed to .tsv
            logger.info(f"Merged dataset saved to {merged_output}")

            return result_df

    # Record start time
    start_time = time.time()

    # Process the DataFrame with optimized parallel processing
    result_df = process_dataframe_parallel(df, batch_size=batch_size, max_workers=max_workers)

    # Post-process results
    result_df = post_process_results(result_df)

    # Record end time and log duration
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Processing completed in {duration:.2f} seconds ({duration/60:.2f} minutes)")

    # Save results
    result_df.to_csv(output_file, sep='\t', index=False)  # Changed to .tsv
    logger.info(f"Results saved to {output_file}")

    # Create merged dataset with dates added to original data
    merged_df = merge_with_original(original_df, result_df)
    merged_output = f"{os.path.splitext(output_file)[0]}.tsv"  # Changed to .tsv
    merged_df.to_csv(merged_output, sep='\t', index=False)  # Changed to .tsv
    logger.info(f"Merged dataset saved to {merged_output}")

    # Save a timestamp-versioned backup
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    backup_file = f"{output_file.rsplit('.', 1)[0]}_{timestamp}.tsv"  # Changed to .tsv
    result_df.to_csv(backup_file, sep='\t', index=False)  # Changed to .tsv
    logger.info(f"Backup saved to {backup_file}")

    return result_df


if __name__ == "__main__":
    # Configuration
    input_file = '../Datasets/haunted_places.tsv'  # Changed to .tsv
    output_file = '../Datasets/haunted_places_evidence_date.tsv'  # Changed to .tsv

    # Optimize parameters (reduce if running out of memory)
    batch_size = 20
    max_workers = 10

    # set reset to True if you want to start from scratch and clear caches
    main(input_file, 
         output_file, 
         batch_size, 
         max_workers, 
         resume=True, 
         reset=True, # set to True if you want to start from scratch
         skip_problematic=True)
    