import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
from datetime import datetime

def scrape_astronomy_data():
    url = "https://www.timeanddate.com/astronomy/usa"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml",
        "Accept-Language": "en-US,en;q=0.9"
    }
    
    try:
        print(f"Requesting data from {url}...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        print(f"Status code: {response.status_code}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table with class "zebra fw tb-sm zebra"
        table = soup.find('table', class_='zebra fw tb-sm zebra')
        if not table:
            print("Target table not found on the page")
            return {}
        
        # Dictionary to store state abbreviation to day duration mapping
        state_to_day_duration = {}
        
        # Skip the header row and process all other rows
        rows = table.find_all('tr')[1:]
        print(f"Found {len(rows)} data rows")
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 9:  # Each row has 9 columns (3 cities, each with name, sunrise, sunset)
                # Process each city in the row (3 cities per row)
                for i in range(0, 9, 3):
                    if i + 2 < len(cols):
                        city_col = cols[i]
                        sunrise_col = cols[i+1]
                        sunset_col = cols[i+2]
                        
                        city_link = city_col.find('a')
                        if city_link:
                            city_state = city_link.text.strip()
                            
                            # Extract the state abbreviation from the city_state (e.g., "Adak (AK)" -> "AK")
                            state_match = re.search(r'\(([A-Z]{2})\)', city_state)
                            if state_match:
                                state_abbrev = state_match.group(1)
                                
                                # Extract sunrise and sunset
                                sunrise = sunrise_col.text.strip().replace('↑', '').strip()
                                sunset = sunset_col.text.strip().replace('↓', '').strip()
                                
                                # Parse times and calculate day duration
                                try:
                                    sunrise_time = datetime.strptime(sunrise, "%I:%M %p")
                                    sunset_time = datetime.strptime(sunset, "%I:%M %p")
                                    
                                    # Calculate difference in hours and minutes
                                    diff_seconds = (sunset_time - sunrise_time).seconds
                                    hours, remainder = divmod(diff_seconds, 3600)
                                    minutes = remainder // 60
                                    day_duration = f"{hours}h {minutes}m"
                                    
                                    # Store the day duration for this state
                                    if state_abbrev not in state_to_day_duration:
                                        state_to_day_duration[state_abbrev] = []
                                    state_to_day_duration[state_abbrev].append((hours, minutes))
                                    
                                    print(f"Found: {city_state} ({state_abbrev}), Day duration: {day_duration}")
                                    
                                except ValueError as e:
                                    print(f"Error parsing time for {city_state}: {e}")
                
            # Add a small delay to be respectful
            time.sleep(random.uniform(0.1, 0.3))
        
        # Calculate average duration for each state
        state_day_duration_map = {}
        for state, durations in state_to_day_duration.items():
            # Convert each duration to total minutes
            total_minutes = [h * 60 + m for h, m in durations]
            
            # Calculate average and convert back to "Xh Ym" format
            if total_minutes:
                avg_minutes = sum(total_minutes) / len(total_minutes)
                avg_hours, avg_mins = divmod(round(avg_minutes), 60)
                state_day_duration_map[state] = f"{avg_hours}h {avg_mins}m"
                print(f"Average day duration for {state}: {avg_hours}h {avg_mins}m (from {len(durations)} data points)")
        
        return state_day_duration_map
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return {}

def add_day_duration_to_haunted_places():
    # Load haunted places data
    haunted_places_path = "../Datasets/haunted_places.tsv"
    print(f"Loading haunted places data from {haunted_places_path}...")
    haunted_places = pd.read_csv(haunted_places_path, sep='\t')
    
    # Scrape astronomy data
    state_day_duration_map = scrape_astronomy_data()
    
    if not state_day_duration_map:
        print("No astronomy data available. Cannot proceed.")
        return
    
    print(f"Found day duration data for {len(state_day_duration_map)} states")
    
    # Add day_duration to haunted_places based on state_abbrev
    print("Adding day duration to haunted places data...")
    haunted_places['day_duration'] = haunted_places['state_abbrev'].map(state_day_duration_map)
    
    # Save the result to a new TSV file
    output_path = "../Datasets/haunted_places_evidence_day_duration.tsv"
    print(f"Saving data to {output_path}...")
    haunted_places.to_csv(output_path, sep='\t', index=False)
    
    mapped_count = sum(haunted_places['day_duration'].notna())
    print(f"Successfully added day duration for {mapped_count} out of {len(haunted_places)} haunted places.")
    print(f"Result saved to {output_path}")

if __name__ == "__main__":
    print("Starting to add day duration data to haunted places...")
    add_day_duration_to_haunted_places()