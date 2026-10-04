import json
import requests

def get_dimensions():
    endpoint = 'https://ghoapi.azureedge.net/api/Dimension'
    response = requests.get(endpoint)

    try:
        data = response.json()
        json_file = open("bronze_layer/dimensions.json", "w")
        json.dump(data, json_file, indent = 4)
        print(data)

        json_file.close()

    except:
        print("Unable to fetch data from api endpoint for some reason")


def get_indicators():
    endpoint = "https://ghoapi.azureedge.net/api/Indicator"
    response = requests.get(endpoint)

    try:
        data = response.json()
        json_file = open("bronze_layer/indicators.json", "w")
        json.dump(data, json_file, indent = 4)
        print(data)

        json_file.close()

    except:
        print("Unable to fetch data from api endpoint for some reason")


get_dimensions()
get_indicators()
