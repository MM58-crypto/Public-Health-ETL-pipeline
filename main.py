import json
import logging
from pathlib import Path
import requests

url = 'https://ghoapi.azureedge.net'
logger = logging.getLogger(__name__)

## Phase 1 -- Extraction

def fetch_data(filename, ep):
    """Save one OData collection response; return whether ingestion succeeded."""
    try:
        with requests.get(ep, timeout=(10, 60)) as response:
            response.raise_for_status()
            data = response.json()
    except requests.exceptions.Timeout as error:
        logger.error("%s: request timed out (%s): %s", filename, ep, error)
        return False
    except requests.exceptions.HTTPError as error:
        logger.error("%s: HTTP request failed (%s): %s", filename, ep, error)
        return False
    except requests.exceptions.JSONDecodeError as error:
        logger.error("%s: invalid JSON response (%s): %s", filename, ep, error)
        return False
    except requests.exceptions.RequestException as error:
        logger.error("%s: network request failed (%s): %s", filename, ep, error)
        return False

    if not isinstance(data, dict) or not isinstance(data.get("value"), list):
        logger.error("%s: expected an OData object with a 'value' list (%s)", filename, ep)
        return False

    output_path = Path("bronze_layer") / f"{filename}.json"
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4)
    except OSError as error:
        logger.error("%s: unable to write %s: %s", filename, output_path, error)
        return False

    logger.info("%s: saved %d records to %s", filename, len(data["value"]), output_path)
    return True


def get_all_dimensions():
    endpoint = f'{url}/api/Dimension'  
    return fetch_data("dimensions", endpoint)


def get_all_indicators():
    endpoint = f"{url}/api/Indicator"
    return fetch_data("indicators", endpoint)

#  to retrieve the list of the COUNTRY dimension values
def get_country_vals():
    endpoint = f"{url}/api/DIMENSION/COUNTRY/DimensionValues"
    return fetch_data("country_vals", endpoint)

# retrieve data for this Indicator: WHOSIS_000004, Adult mortality rate
def get_adult_mortality():
    endpoint = f"{url}/api/WHOSIS_000004"
    return fetch_data("adult_mortality", endpoint)

# retrieve data for this Indicator: WHOSIS_000001, Life expectancy at birth
def get_le_birth():
    endpoint = f"{url}/api/WHOSIS_000001"
    return fetch_data("life_expectancy_at_birth", endpoint)

# retrieve data for this Indicator: WHOSIS_000002, Healthy life expectancy at birth
def get_hale_birth():
    endpoint = f"{url}/api/WHOSIS_000002"
    return fetch_data("hale_at_birth", endpoint)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    datasets = (
        get_all_dimensions,
        get_all_indicators,
        get_country_vals,
        get_adult_mortality,
        get_le_birth,
        get_hale_birth,
    )
    failures = 0
    for ingest in datasets:
        if not ingest():
            failures += 1

    if failures:
        logger.error("Ingestion failed for %d of %d datasets.", failures, len(datasets))
        return 1
    logger.info("All %d datasets saved successfully.", len(datasets))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

