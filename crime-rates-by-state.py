import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Base API URL for fetching crime data by state
BASE_URL = "https://projects.csgjusticecenter.org/tools-for-states-to-address-crime/50-state-crime-data/?state={}"

# State abbreviations
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

# List to store extracted data
crime_data = []

# Loop through each state abbreviation
for state_abbr, state_name in STATE_ABBREVIATIONS.items():
    print(f"Fetching crime data for {state_name} ({state_abbr})...")

    url = BASE_URL.format(state_abbr)
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to fetch data for {state_name}. Skipping...")
        continue

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract data using string search
    try:
        violent_crime_text = soup.find(string=lambda t: t and "violent crime incidents per 100,000 residents" in t.lower())
        murder_rate_text = soup.find(string=lambda t: t and "murder rate per 100,000" in t.lower())
        police_reports_text = soup.find(string=lambda t: t and "police reports per capita" in t.lower())

        # Extract numeric values
        violent_crime_rate = violent_crime_text.split()[0] if violent_crime_text else "N/A"
        murder_rate = murder_rate_text.split()[0] if murder_rate_text else "N/A"
        police_reports_per_capita = police_reports_text.split()[0] if police_reports_text else "N/A"

        # Append data
        crime_data.append({
            "State": state_name,
            "Violent_Crime_Rate": violent_crime_rate,
            "Murder_Rate": murder_rate,
            "Police_Reports_Per_Capita": police_reports_per_capita
        })

        print(f"Successfully extracted data for {state_name}.")

    except Exception as e:
        print(f"Could not extract data for {state_name}: {e}")


    # Delay to avoid rate limiting
    time.sleep(2)

# Convert to DataFrame
df_crime = pd.DataFrame(crime_data)

# Save to CSV
df_crime.to_csv("/work/Exports/crime_rates_data/crime_data_by_state.csv", index=False)
print("Data extraction complete! Saved as 'crime_data_by_state.csv'.")

# Display DataFrame
# import ace_tools as tools
# tools.display_dataframe_to_user(name="Crime Data by State", dataframe=df_crime)
