import json
import requests

url = 'https://ghoapi.azureedge.net'

## Phase 1 -- Extraction
def get_dimensions():
    endpoint = f'{url}/api/Dimension'
    response = requests.get(endpoint)

    try:
        data = response.json()
        json_file = open("bronze_layer/dimensions.json", "w")
        json.dump(data, json_file, indent = 4)
        #print(data)

        json_file.close()

    except:
        print("Unable to fetch data from api endpoint for some reason")


def get_indicators():
    endpoint = f"{url}/api/Indicator"
    response = requests.get(endpoint)

    try:
        data = response.json()
        json_file = open("bronze_layer/indicators.json", "w")
        json.dump(data, json_file, indent = 4)
        #print(data)

        json_file.close()

    except:
        print("Unable to fetch data from api endpoint for some reason")

#  to retrieve the list of the COUNTRY dimension values
def get_country_vals():
    endpoint = f"{url}/api/DIMENSION/COUNTRY/DimensionValues"
    response = requests.get(endpoint)

    try:
        data = response.json()
        json_file = open("bronze_layer/country_vals.json", "w")
        json.dump(data, json_file, indent = 4)
        #print(data)

        json_file.close()

    except:
        print("Unable to fetch data from api endpoint for some reason")


get_dimensions()
get_indicators()
get_country_vals()
