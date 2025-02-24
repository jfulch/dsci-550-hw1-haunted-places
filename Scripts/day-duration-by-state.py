import aiohttp
import asyncio
import nest_asyncio
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib.parse

nest_asyncio.apply()

async def fetch_daylight_data(session, latitude, longitude, date):
    base_url = "https://aa.usno.navy.mil/calculated/rstt/oneday"
    params = {
        "date": date.strftime("%Y-%m-%d"),
        "lat": f"{latitude:.4f}",
        "lon": f"{longitude:.4f}",
        "label": "test",
        "tz": 0.00,
        "tz_sign": 1,
        "tz_label": "false",
        "dst": "false",
        "submit": "Get Data"
    }
    
    full_url = base_url + "?" + urllib.parse.urlencode(params)
    print(f"Scraping URL: {full_url}")

    try:
        async with session.get(full_url) as response:
            response.raise_for_status()
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            def find_table_with_rise_set(soup):
                tables = soup.find_all('table')
                for table in tables:
                    if 'Rise' in table.text and 'Set' in table.text:
                        return table
                return None

            table = find_table_with_rise_set(soup)
            if not table:
                print("Could not find results table containing 'Rise' and 'Set'")
                return None
            print("Successfully found results table")

            rows = table.find_all('tr')
            if len(rows) < 3:
                print("Could not find enough rows in the table (expected at least 3)")
                return None
            print("Successfully found enough rows in the table")

            sunrise_text = None
            sunset_text = None

            for row in rows:
                cells = row.find_all('td')
                if cells and len(cells) > 0 and cells[0].text.strip() == "Rise":
                    sunrise_text = cells[1].text.strip()
                if cells and len(cells) > 0 and cells[0].text.strip() == "Set":
                    sunset_text = cells[1].text.strip()

            if sunrise_text and sunset_text:
                print(f"Sunrise text: {sunrise_text}, Sunset text: {sunset_text}")
            else:
                print("Could not find Rise or Set times in the table")
                return None

            try:
                sunrise = datetime.strptime(sunrise_text, "%H:%M")
                sunset = datetime.strptime(sunset_text, "%H:%M")
                print("Successfully parsed sunrise and sunset times")

                daylight_duration = datetime.combine(date, sunset.time()) - datetime.combine(date, sunrise.time())
                daylight_hours = daylight_duration.total_seconds() / 3600
                print(f"Daylight duration: {daylight_hours:.2f} hours")
                print(f"Daylight duration (timedelta): {daylight_duration}")
                return daylight_hours

            except ValueError:
                print(f"Could not parse sunrise/sunset times: sunrise='{sunrise_text}', sunset='{sunset_text}'")
                return None

    except aiohttp.ClientError as e:
        print(f"Error scraping {base_url}: {e}")
        return None

async def main():
    print("Starting main function")
    haunted_places_file = "Datasets/haunted_places_evidence.tsv"
    try:
        haunted_df = pd.read_csv(haunted_places_file, sep='\t')
        print(f"Successfully loaded {haunted_places_file}")
    except FileNotFoundError:
        print(f"Error: {haunted_places_file} not found.")
        return

    haunted_df['latitude'] = pd.to_numeric(haunted_df['latitude'], errors='coerce')
    haunted_df['longitude'] = pd.to_numeric(haunted_df['longitude'], errors='coerce')
    print("Successfully converted latitude and longitude to numeric")

    haunted_df['average_daylight_hours'] = None
    today = datetime.now()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, row in haunted_df.iterrows():
            latitude = row['latitude']
            longitude = row['longitude']

            if pd.isna(latitude) or pd.isna(longitude):
                print(f"Skipping row {index} due to invalid latitude or longitude")
                continue

            print(f"Scraping data for latitude: {latitude}, longitude: {longitude}")
            task = asyncio.ensure_future(fetch_daylight_data(session, latitude, longitude, today))
            tasks.append(task)

        daylight_hours = await asyncio.gather(*tasks)

    for index, hours in enumerate(daylight_hours):
        if hours is not None:
            haunted_df.loc[index, 'average_daylight_hours'] = hours
            row = haunted_df.iloc[index]
            latitude = row['latitude']
            longitude = row['longitude']
            print(f"Daylight data for {latitude}, {longitude}: {hours:.2f} hours")
        else:
            row = haunted_df.iloc[index]
            latitude = row['latitude']
            longitude = row['longitude']
            print(f"Could not retrieve daylight data for {latitude}, longitude: {longitude}")
            print('---------')

    output_file = "Exports/daylight_duration_data/haunted_places_evidence_daylight.tsv"
    haunted_df.to_csv(output_file, sep='\t', index=False)
    print(f"Successfully merged and saved data to {output_file}")

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
