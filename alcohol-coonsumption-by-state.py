import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

# Site URL
url = "https://drugabusestatistics.org/alcohol-abuse-statistics/"

# Fetch webpage content
response = requests.get(url)
response.raise_for_status()

# Parse HTML
soup = BeautifulSoup(response.text, "html.parser")

# Extract all text content
text_content = soup.get_text(separator="\n")

# Initialize state_data list
state_data = []

# Function to extract data for a given state
def extract_state_data(state_name, text_content):
    # Extract binge drinking rate
    binge_pattern = re.search(r"(\d+\.\d+|\d+)%\s*of\s*.*?(binge drink|excessive drinking)", text_content, re.IGNORECASE)

    # Extract annual deaths attributable to excessive alcohol use
    deaths_pattern = re.search(r"(An average of\s*)?([\d,]+)\s*(annual )?(deaths?|fatalities).*?attributable to excessive alcohol use", text_content, re.IGNORECASE)

    # Extract number of times adults binge drink monthly
    binge_times_pattern = re.search(r"(Binge drinking adults|Adults who binge drink).*?(\d+\.\d+|\d+)\s*times? (per month|monthly)", text_content, re.IGNORECASE)

    # Assign extracted values, or "N/A" if not found
    binge_rate = binge_pattern.group(1) if binge_pattern else "N/A"
    deaths_alcohol = deaths_pattern.group(2) if deaths_pattern else "N/A"
    binge_times_monthly = binge_times_pattern.group(2) if binge_times_pattern else "N/A"

    return [state_name, 2023, binge_rate, deaths_alcohol, binge_times_monthly]

# Extract data for each state
state_sections = re.split(r'((?:[A-Z][a-z]+(?:\s[A-Z][a-z]+)*) Alcohol Abuse Statistics)', text_content)

# Iterate through state sections and extract data
for i in range(1, len(state_sections), 2):
    state_name = state_sections[i].replace(" Alcohol Abuse Statistics", "").strip()
    state_text = state_sections[i + 1]
    
    # Special handling for District of Columbia
    if state_name == "Columbia":
        state_name = "District of Columbia"
    
    state_data.append(extract_state_data(state_name, state_text))

# Special handling for District of Columbia if not found in the main loop
dc_names = ["District of Columbia", "D.C.", "Washington D.C."]
dc_data = None
for dc_name in dc_names:
    dc_data = extract_state_data(dc_name, text_content)
    if dc_data[2] != "N/A" or dc_data[3] != "N/A" or dc_data[4] != "N/A":
        # Check if District of Columbia is already in state_data
        if not any(data[0] == "District of Columbia" for data in state_data):
            state_data.append(dc_data)
        break

# If D.C. data is still not found, add a placeholder
if dc_data is None or (dc_data[2] == "N/A" and dc_data[3] == "N/A" and dc_data[4] == "N/A"):
    if not any(data[0] == "District of Columbia" for data in state_data):
        state_data.append(["District of Columbia", 2023, "N/A", "N/A", "N/A"])

# Convert to DataFrame
df_alcohol_stats = pd.DataFrame(
    state_data, columns=["State", "Year", "Binge_Drinking_Rate", "Annual_Alcohol_Deaths", "Binge_Times_Monthly"]
)

# Remove any empty rows or artifacts
df_alcohol_stats = df_alcohol_stats[df_alcohol_stats["State"].notna()]

# Display first few rows
print(df_alcohol_stats.head(10))

# Save DataFrame to CSV
df_alcohol_stats.to_csv("Exports/alcohol_data/alcohol_abuse_by_state_2023.csv", index=False)

print("Data extraction complete! Saved as 'alcohol_abuse_by_state_2023.csv'")
