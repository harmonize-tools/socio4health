import pandas as pd

from nyctibius.etl.transformer import Transformer
from nyctibius.harmonizer import Harmonizer
from nyctibius.dto.data_info import DataInfo
from nyctibius.enums.headers_enum import HeadersEnum
from nyctibius.enums.config_enum import ConfigEnum


def main():

    # TODO Erick recibe un Data info con solo URL y Descripción ej: datinfo1 = DataInfo(None, 'My Dataset 1', 'https://example.com')
    # TODO El extractor setea el campo file_path al extraer los archivos en la carpeta

    datinfo1 = DataInfo('../../data/input/Person.csv', 'My Dataset 1', 'https://example.com')
    datainfo2 = DataInfo('../../data/input/Household.csv', 'My Dataset 2', 'https://example.com')

    list_datainfo = [datinfo1, datainfo2]

    # Create a Harmonizer instance
    harmonizer = Harmonizer(list_datainfo)

    # Extract data
    # TODO harmonizer.extract()

    # Transform data
    harmonizer.transform('Person')

    # Load the data
    results = harmonizer.load()

    # Print the results
    for i, result in enumerate(results):
        print(f"Dataset {i + 1}: Success: {result[0]}, Message: {result[1]}")


if __name__ == "__main__":
    main()
