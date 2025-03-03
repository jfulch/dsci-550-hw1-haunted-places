import pandas as pd
import numpy as np

###############################################################################
# State Name Normalization
###############################################################################
# Dictionary maps various representations (abbreviations or full names) to a consistent, lowercase full state name.
unify_states = {
    'al': 'alabama', 'alabama': 'alabama',
    'ak': 'alaska', 'alaska': 'alaska',
    'az': 'arizona', 'arizona': 'arizona',
    'ar': 'arkansas', 'arkansas': 'arkansas',
    'ca': 'california', 'california': 'california',
    'co': 'colorado', 'colorado': 'colorado',
    'ct': 'connecticut', 'connecticut': 'connecticut',
    'de': 'delaware', 'delaware': 'delaware',
    'fl': 'florida', 'florida': 'florida',
    'ga': 'georgia', 'georgia': 'georgia',
    'hi': 'hawaii', 'hawaii': 'hawaii',
    'id': 'idaho', 'idaho': 'idaho',
    'il': 'illinois', 'illinois': 'illinois',
    'in': 'indiana', 'indiana': 'indiana',
    'ia': 'iowa', 'iowa': 'iowa',
    'ks': 'kansas', 'kansas': 'kansas',
    'ky': 'kentucky', 'kentucky': 'kentucky',
    'la': 'louisiana', 'louisiana': 'louisiana',
    'me': 'maine', 'maine': 'maine',
    'md': 'maryland', 'maryland': 'maryland',
    'ma': 'massachusetts', 'massachusetts': 'massachusetts',
    'mi': 'michigan', 'michigan': 'michigan',
    'mn': 'minnesota', 'minnesota': 'minnesota',
    'ms': 'mississippi', 'mississippi': 'mississippi',
    'mo': 'missouri', 'missouri': 'missouri',
    'mt': 'montana', 'montana': 'montana',
    'ne': 'nebraska', 'nebraska': 'nebraska',
    'nv': 'nevada', 'nevada': 'nevada',
    'nh': 'new hampshire', 'new hampshire': 'new hampshire',
    'nj': 'new jersey', 'new jersey': 'new jersey',
    'nm': 'new mexico', 'new mexico': 'new mexico',
    'ny': 'new york', 'new york': 'new york',
    'nc': 'north carolina', 'north carolina': 'north carolina',
    'nd': 'north dakota', 'north dakota': 'north dakota',
    'oh': 'ohio', 'ohio': 'ohio',
    'ok': 'oklahoma', 'oklahoma': 'oklahoma',
    'or': 'oregon', 'oregon': 'oregon',
    'pa': 'pennsylvania', 'pennsylvania': 'pennsylvania',
    'ri': 'rhode island', 'rhode island': 'rhode island',
    'sc': 'south carolina', 'south carolina': 'south carolina',
    'sd': 'south dakota', 'south dakota': 'south dakota',
    'tn': 'tennessee', 'tennessee': 'tennessee',
    'tx': 'texas', 'texas': 'texas',
    'ut': 'utah', 'utah': 'utah',
    'vt': 'vermont', 'vermont': 'vermont',
    'va': 'virginia', 'virginia': 'virginia',
    'wa': 'washington', 'washington': 'washington',
    'wv': 'west virginia', 'west virginia': 'west virginia',
    'wi': 'wisconsin', 'wisconsin': 'wisconsin',
    'wy': 'wyoming', 'wyoming': 'wyoming',
    'dc': 'district of columbia', 'district of columbia': 'district of columbia'
}

def unify_state_col(series: pd.Series) -> pd.Series:
    """
    Convert state strings to lowercase, strip spaces, and map using unify_states.
    """
    return series.str.lower().str.strip().map(unify_states).fillna(series.str.lower().str.strip())

###############################################################################
# Year Extraction Function for Haunted Data
###############################################################################
def extract_year(val) -> int:
    """
    If val contains '/', assume it's a date string (e.g. m/d/yy or m/d/yyyy)
    and extract the last component as the year. If that year is two digits (<100),
    assume it's 2000+year.
    If val is already numeric, return it as an integer.
    """
    if pd.isna(val):
        return np.nan
    if isinstance(val, (int, float)):
        return int(val)
    val = str(val).strip()
    if '/' in val:
        parts = val.split('/')
        try:
            year_val = int(parts[-1])
            if year_val < 100:
                return 2000 + year_val
            return year_val
        except:
            return np.nan
    else:
        try:
            y = int(val)
            if y < 100:
                return 2000 + y
            return y
        except:
            return np.nan

###############################################################################
# Standardize State and Year Columns
###############################################################################
def standardize_state_and_year(df: pd.DataFrame, file_type: str = "haunted", year_value: int = None) -> pd.DataFrame:
    df.columns = df.columns.str.lower().str.strip()
    
    if 'state' in df.columns:
        df['state'] = unify_state_col(df['state'])
    
    if file_type == 'haunted':
        if 'haunted_places_date' in df.columns:
            df = df[df['haunted_places_date'] != '1/1/2025']
            df['year'] = df['haunted_places_date'].apply(extract_year)
        elif 'year' in df.columns:
            df['year'] = df['year'].apply(extract_year)
    else:
        if 'year' in df.columns:
            df['year'] = df['year'].apply(extract_year)
        elif year_value is not None:
            df['year'] = year_value
    return df

###############################################################################
# Desired Final Columns 
###############################################################################
DESIRED_COLUMNS = [
    "State",
    "Haunted_Places_Year",
    "Audio_Evidence_Count",
    "Visual_Evidence_Count",
    "Haunted_Places_Witness_Count",
    "Morning_Event_Count",
    "Evening_Event_Count",
    "Dusk_Event_Count",
    "Appar_Orb_Count",
    "Appar_Ghost_Count",
    "Appar_UFO_Count",
    "Murder_Event_Count",
    "Death_Event_Count",
    "Supernatural_Event_Count",
    "Avg_Daylight_Hours_In_Year",
    "Binge_Drinking_Rate",
    "Annual_Alcohol_Deaths",
    "Binge_Times_Monthly",
    "Violent_Crime_Per_100k",
    "Percent_Of_Crime_Solved",
    "Homicide_Incidents_Per_100K",
    "Anxiety_Disorder_Count",
    "Attention_Deficit/Hyperactivity_Disorder_Count",
    "Bipolar_Disorder_Count",
    "Primary_Depressive_Disorder_Count",
    "Trauma_And_Stressor_Related_Disorder_Count",
    "Ozone_ppb",
    "SO2_ppb",
    "CO_ppb",
    "PM25_ugm3",
    "PM25_sulfate_ugm3",
    "PM25_nitrate_ugm3",
    "Extreme_Precipitation_Event_Rate",
    "Precipitation_Impact_Rate",
    "Precipitation_Variability"
]

###############################################################################
# 1. Haunted Places Data (City-Level)
###############################################################################
def load_and_aggregate_haunted(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, sep='\t', dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    
    if 'evidence_date' in df.columns:
        df = df[df['evidence_date'] != '1/1/2025']
        df['year'] = df['evidence_date'].apply(extract_year)
    if 'year' in df.columns:
        df['year'] = df['year'].apply(extract_year)
    df = df[df['year'].notna()]
    df['year'] = df['year'].astype(int)
    df = df[(df['year'] >= 2015) & (df['year'] <= 2025)]
    
    if 'state' in df.columns:
        df['state'] = unify_state_col(df['state'])
    
    df['audio_evidence_bool'] = df['audio_evidence'].str.lower().isin(['true', '1'])
    df['visual_evidence_bool'] = df['visual_evidence'].str.lower().isin(['true', '1'])
    
    df['haunted_places_witness_count'] = pd.to_numeric(df.get('haunted_places_witness_count'), errors='coerce')
    df['average_daylight_hours'] = pd.to_numeric(df.get('average_daylight_hours'), errors='coerce')
    
    group_cols = ['state', 'year']
    grouped = df.groupby(group_cols, dropna=True).agg({
        'audio_evidence_bool': 'sum',
        'visual_evidence_bool': 'sum',
        'haunted_places_witness_count': 'sum',
        'average_daylight_hours': 'mean'
    }).reset_index()
    grouped.rename(columns={
        'audio_evidence_bool': 'Audio_Evidence_Count',
        'visual_evidence_bool': 'Visual_Evidence_Count',
        'haunted_places_witness_count': 'Haunted_Places_Witness_Count',
        'average_daylight_hours': 'Avg_Daylight_Hours_In_Year'
    }, inplace=True)
    
    def count_by_val(df_in, col, val, out_name):
        mask = df_in[col].str.lower().str.strip() == val
        return df_in[mask].groupby(group_cols).size().reset_index(name=out_name)
    
    morning_df = count_by_val(df, 'time_of_day', 'morning', 'Morning_Event_Count')
    evening_df = count_by_val(df, 'time_of_day', 'evening', 'Evening_Event_Count')
    dusk_df = count_by_val(df, 'time_of_day', 'dusk', 'Dusk_Event_Count')
    
    orb_df = count_by_val(df, 'apparition_type', 'orb', 'Appar_Orb_Count')
    ghost_df = count_by_val(df, 'apparition_type', 'ghost', 'Appar_Ghost_Count')
    ufo_df = count_by_val(df, 'apparition_type', 'ufo', 'Appar_UFO_Count')
    
    def count_by_event(df_in, val, out_name):
        mask = df_in['event_type'].str.lower().str.strip() == val
        return df_in[mask].groupby(group_cols).size().reset_index(name=out_name)
    
    murder_df = count_by_event(df, 'murder', 'Murder_Event_Count')
    death_df = count_by_event(df, 'death', 'Death_Event_Count')
    sup_df = df[df['event_type'].str.lower().str.strip().str.contains('supernatural', na=False)]\
             .groupby(group_cols).size().reset_index(name='Supernatural_Event_Count')
    
    for extra_df in [morning_df, evening_df, dusk_df, orb_df, ghost_df, ufo_df, murder_df, death_df, sup_df]:
        grouped = pd.merge(grouped, extra_df, on=group_cols, how='left')
    
    for col in ['Morning_Event_Count','Evening_Event_Count','Dusk_Event_Count',
                'Appar_Orb_Count','Appar_Ghost_Count','Appar_UFO_Count',
                'Murder_Event_Count','Death_Event_Count','Supernatural_Event_Count']:
        if col in grouped.columns:
            grouped[col] = grouped[col].fillna(0)
    
    return grouped

###############################################################################
# 2. Crime Data
###############################################################################
def load_crime_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, sep='\t', dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    if 'state' in df.columns:
        df['state'] = unify_state_col(df['state'])
    if 'year' in df.columns:
        df['year'] = df['year'].apply(extract_year)
    for col in ['violent_crime_per_100k','percent_of_crimes_solved','homicide_incidents_per_100k']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    group_cols = ['state', 'year']
    grouped = df.groupby(group_cols, dropna=True)[['violent_crime_per_100k','percent_of_crimes_solved','homicide_incidents_per_100k']].mean().reset_index()
    rename_map = {
        'violent_crime_per_100k': 'Violent_Crime_Per_100k',
        'percent_of_crimes_solved': 'Percent_Of_Crime_Solved',
        'homicide_incidents_per_100k': 'Homicide_Incidents_Per_100K'
    }
    grouped.rename(columns=rename_map, inplace=True)
    return grouped

###############################################################################
# 3. Mental Health Data
###############################################################################
def load_mental_health_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, sep='\t', dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    if 'state' in df.columns:
        df['state'] = unify_state_col(df['state'])
    if 'year' in df.columns:
        df['year'] = df['year'].apply(extract_year)
    df.columns = [c.replace(' ', '_').replace('-', '_') for c in df.columns]
    num_cols = ['anxiety_disorders','attention_deficit/hyperactivity_disorder','bipolar_disorders',
                'primary_depressive_disorders','trauma__and_stressor_related_disorders']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    group_cols = ['state', 'year']
    grouped = df.groupby(group_cols, dropna=True)[num_cols].mean().reset_index()
    rename_map = {
        'anxiety_disorders': 'Anxiety_Disorder_Count',
        'attention_deficit/hyperactivity_disorder': 'Attention_Deficit/Hyperactivity_Disorder_Count',
        'bipolar_disorders': 'Bipolar_Disorder_Count',
        'primary_depressive_disorders': 'Primary_Depressive_Disorder_Count',
        'trauma__and_stressor_related_disorders': 'Trauma_And_Stressor_Related_Disorder_Count'
    }
    grouped.rename(columns=rename_map, inplace=True)
    return grouped

###############################################################################
# 4. Weather Data
###############################################################################
def load_weather_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, sep='\t', dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    if 'state' in df.columns:
        df['state'] = unify_state_col(df['state'])
    if 'year' in df.columns:
        df['year'] = df['year'].apply(extract_year)
    else:
        df['year'] = 2023
    wcols = ['extreme_precipitation_event_rate','precipitation_impact_rate','precipitation_variability']
    for col in wcols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    group_cols = ['state', 'year']
    grouped = df.groupby(group_cols, dropna=True)[wcols].mean().reset_index()
    rename_map = {
        'extreme_precipitation_event_rate': 'Extreme_Precipitation_Event_Rate',
        'precipitation_impact_rate': 'Precipitation_Impact_Rate',
        'precipitation_variability': 'Precipitation_Variability'
    }
    grouped.rename(columns=rename_map, inplace=True)
    return grouped

###############################################################################
# 5. Alcohol Data
###############################################################################
def load_alcohol_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, sep='\t', dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    if 'state' in df.columns:
        df['state'] = unify_state_col(df['state'])
    if 'year' in df.columns:
        df['year'] = df['year'].apply(extract_year)
    acols = ['binge_drinking_rate','annual_alcohol_deaths','binge_times_monthly']
    for col in acols:
        if col in df.columns:
            df[col] = df[col].str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    group_cols = ['state', 'year']
    grouped = df.groupby(group_cols, dropna=True)[acols].mean().reset_index()
    return grouped

###############################################################################
# 6. Air Quality Data
###############################################################################
def load_air_quality_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, sep='\t', dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    if 'state' in df.columns:
        df['state'] = unify_state_col(df['state'])
    if 'year' in df.columns:
        df['year'] = df['year'].apply(extract_year)
    else:
        df['year'] = 2023
    aq_cols = ['ozone_ppb','so2_ppb','co_ppb','pm25_ugm3','pm25_sulfate_ugm3','pm25_nitrate_ugm3']
    for col in aq_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    group_cols = ['state', 'year']
    grouped = df.groupby(group_cols, dropna=True)[aq_cols].mean().reset_index()
    rename_map = {
        'ozone_ppb': 'Ozone_ppb',
        'so2_ppb': 'SO2_ppb',
        'co_ppb': 'CO_ppb',
        'pm25_ugm3': 'PM25_ugm3',
        'pm25_sulfate_ugm3': 'PM25_sulfate_ugm3',
        'pm25_nitrate_ugm3': 'PM25_nitrate_ugm3'
    }
    grouped.rename(columns=rename_map, inplace=True)
    return grouped

###############################################################################
# MAIN MERGE
###############################################################################
def main():
    # File paths
    haunted_path = "/work/Datasets/haunted_places_evidence_combined_output.tsv"
    crime_path   = "/work/Exports/crime_rates_data/crime_data_by_state.tsv"
    mh_path      = "/work/Exports/mental_health_data/mental_health_conditions_by_state_2017_2022.tsv"
    weather_path = "/work/Exports/weather_condition_data/weather_data_by_state_2023.tsv"
    alcohol_path = "/work/Exports/alcohol_data/alcohol_abuse_by_state.tsv"
    aq_path      = "/work/Exports/air_pollution_data/air_quality_all_states_2023.tsv"
    
    df_haunted = load_and_aggregate_haunted(haunted_path)
    df_crime = load_crime_data(crime_path)
    df_mh = load_mental_health_data(mh_path)
    df_weather = load_weather_data(weather_path)
    df_alcohol = load_alcohol_data(alcohol_path)
    df_aq = load_air_quality_data(aq_path)
    
    # Merge on (state, year) using left joins; haunted data is primary
    merged = df_haunted.copy()
    for other_df in [df_crime, df_mh, df_weather, df_alcohol, df_aq]:
        merged = pd.merge(merged, other_df, on=['state','year'], how='left')
    
    # Rename keys for final output
    merged.rename(columns={'state': 'State', 'year': 'Haunted_Places_Year'}, inplace=True)
    
    # Ensure all desired columns exist
    for col in DESIRED_COLUMNS:
        if col not in merged.columns:
            merged[col] = np.nan
    merged = merged[DESIRED_COLUMNS]
    
    # Sort by State alphabetically, then Haunted_Places_Year descending
    merged = merged.sort_values(by=['State', 'Haunted_Places_Year'], ascending=[True, False])
    
    print("Final merged shape:", merged.shape)
    print(merged[['State','Haunted_Places_Year']].drop_duplicates().head(15))
    
    # Save to TSV
    out_path = "/work/Exports/merged_data/merged_dataset.tsv"
    merged.to_csv(out_path, sep='\t', index=False)
    print(f"Saved final merged dataset to {out_path}")

if __name__ == "__main__":
    main()
