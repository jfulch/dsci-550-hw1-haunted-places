import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re

BASE_URL = "https://csg-state-violent-crime.netlify.app/state-viol-crime-{}"

STATE_ABBREVIATIONS = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado",
    "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"
}

crime_data = []

for state_abbr, state_name in STATE_ABBREVIATIONS.items():
    print(f"Fetching crime data for {state_name} ({state_abbr})...")

    url = BASE_URL.format(state_abbr.lower())
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to fetch data for {state_name}. Skipping...")
        continue

    soup = BeautifulSoup(response.text, "html.parser")

    try:
        violent_crime_text = soup.find(string=lambda t: t and "violent crime incidents per 100,000 residents were reported to police in " + state_name in t)
        crimes_solved_text = soup.find(string=lambda t: t and "percent of violent crimes reported to police in " + state_name + " were solved" in t)

        year = "N/A"
        violent_crime_rate = "N/A"
        percent_solved = "N/A"
        homicide_rate = "N/A"

        if violent_crime_text:
            year_match = re.search(r'In (\d{4}),', violent_crime_text)
            year = year_match.group(1) if year_match else "N/A"
            
            rate_match = re.search(r'In \d{4}, (\d+) violent crime incidents', violent_crime_text)
            violent_crime_rate = rate_match.group(1) if rate_match else "N/A"

        if crimes_solved_text:
            solved_match = re.search(r'In \d{4}, (\d+) percent of violent crimes', crimes_solved_text)
            percent_solved = solved_match.group(1) if solved_match else "N/A"

        # Extract homicide rate from the JSON data in the script tag
        script_tag = soup.find('script', {'data-for': lambda x: x and x.startswith('htmlwidget-')})
        if script_tag:
            json_text = script_tag.string
            data = json.loads(json_text)
            series_data = data["x"]["hc_opts"]["series"]
            state_data = next((s for s in series_data if s.get("name") == state_name), None)
            if state_data:
                data_2023 = next((d for d in state_data["data"] if d.get("year") == 2023), None)
                if data_2023:
                    homicide_rate = data_2023["incidents_reported_rate_total"]

        crime_data.append({
            "State": state_name,
            "Year": year,
            "Violent_Crime_Per_100k": violent_crime_rate,
            "Percent_of_Crimes_Solved": percent_solved,
            "Homicide_Incidents_Per_100k": homicide_rate
        })

        print(f"Successfully extracted data for {state_name}: Year = {year}, Violent Crime Rate = {violent_crime_rate}, Percent Solved = {percent_solved}, Homicide Rate = {homicide_rate}")

    except Exception as e:
        print(f"Could not extract data for {state_name}: {e}")

df_crime = pd.DataFrame(crime_data)
df_crime.to_csv("Exports/crime_rates_data/crime_data_by_state.csv", index=False)
print("Data extraction complete! Saved as 'crime_data_by_state.csv'.")
print(df_crime)
