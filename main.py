import json
import requests

url = 'https://ghoapi.azureedge.net'

## Phase 1 -- Extraction
def get_all_dimensions():
    endpoint = f'{url}/api/Dimension'
    

    try:
        data = response.json()
        response = requests.get(endpoint)
        json_file = open("bronze_layer/dimensions.json", "w")
        json.dump(data, json_file, indent = 4)
        #print(data)
        
        json_file.close()
        print("Data ingested successfully!")
    except:
        print("Unable to fetch data from api endpoint for some reason")


def get_all_indicators():
    endpoint = f"{url}/api/Indicator"
    

    try:
        response = requests.get(endpoint)
        data = response.json()
        json_file = open("bronze_layer/indicators.json", "w")
        json.dump(data, json_file, indent = 4)
        #print(data)

        json_file.close()
        print("Data ingested successfully!")
    except:
        print("Unable to fetch data from api endpoint for some reason")

# retrieve data for this Indicator: WHOSIS_000004, Adult mortality rate
def get_adult_mortality():
    endpoint = f"{url}/api/WHOSIS_000004"
    

    try:
        response = requests.get(endpoint)
        data = response.json()
        json_file = open("bronze_layer/adult_mortality.json", "w")
        json.dump(data, json_file, indent = 4)
        #print(data)

        json_file.close()
        print("Data ingested successfully!")
    except:
        print("Unable to fetch data from api endpoint for some reason")

#  to retrieve the list of the COUNTRY dimension values
def get_country_vals():
    endpoint = f"{url}/api/DIMENSION/COUNTRY/DimensionValues"
    

    try:
        response = requests.get(endpoint)
        data = response.json()
        json_file = open("bronze_layer/country_vals.json", "w")
        json.dump(data, json_file, indent = 4)
        #print(data)

        json_file.close()
        print("Data ingested successfully!")

    except:
        print("Unable to fetch data from api endpoint for some reason")


get_all_dimensions()
get_all_indicators()
get_adult_mortality()
get_country_vals()
