import json
import requests

url = 'https://ghoapi.azureedge.net'

## Phase 1 -- Extraction

def fetch_data(filename, ep):
    
    try:
        response = requests.get(ep)
        data = response.json()
        if response.status_code == 200:

            json_file = open(f"bronze_layer/{filename}.json", "w")
            json.dump(data, json_file, indent = 4)
            json_file.close()
        else:
            return None
        print(f"{filename} data ingested successfully!")
    except Exception as e:
        print("Unable to fetch data from api endpoint: ", e)


def get_all_dimensions():
    endpoint = f'{url}/api/Dimension'  
    fetch_data("dimensions", endpoint)


def get_all_indicators():
    endpoint = f"{url}/api/Indicator"
    fetch_data("indicators", endpoint)

#  to retrieve the list of the COUNTRY dimension values
def get_country_vals():
    endpoint = f"{url}/api/DIMENSION/COUNTRY/DimensionValues"
    fetch_data("country_vals", endpoint)

# retrieve data for this Indicator: WHOSIS_000004, Adult mortality rate
def get_adult_mortality():
    endpoint = f"{url}/api/WHOSIS_000004"
    fetch_data("adult_mortality", endpoint)

# retrieve data for this Indicator: WHOSIS_000001, Life expectancy at birth
def get_le_birth():
    endpoint = f"{url}/api/WHOSIS_000001"
    fetch_data("life_expectancy_at_birth", endpoint)

# retrieve data for this Indicator: WHOSIS_0000015, Healthy life expectancy at birth
def get_hle_birth():
    endpoint = f"{url}/api/WHOSIS_000015"
    fetch_data("h_life_expectancy_at_birth", endpoint)


get_all_dimensions()
get_all_indicators()
get_country_vals()
get_adult_mortality()
get_le_birth()
get_hle_birth()

