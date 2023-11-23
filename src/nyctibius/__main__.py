import pandas as pd

from nyctibius.etl.transformer import Transformer
from nyctibius.harmonizer import Harmonizer
from nyctibius.dto.data_info import DataInfo
from nyctibius.enums.headers_enum import HeadersEnum
from nyctibius.enums.config_enum import ConfigEnum


def main():

    # Create a Harmonizer instance
    harmonizer = Harmonizer()
    # Extract data
    My_url = 'https://microdatos.dane.gov.co/index.php/catalog/643/get_microdata'
    list_datainfo = harmonizer.extract(url=My_url)
    harmonizer = Harmonizer(list_datainfo)

    # Transform data
    #harmonizer.transform('Person')

    # Load the data
    #results = harmonizer.load()

    # Print the results
    #for i, result in enumerate(results):
    #    print(f"Dataset {i + 1}: Success: {result[0]}, Message: {result[1]}")


if __name__ == "__main__":
    main()
