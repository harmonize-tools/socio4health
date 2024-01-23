import pandas as pd

from etl.transformer import Transformer
from harmonizer import Harmonizer
from dto.data_info import DataInfo
from enums.headers_enum import HeadersEnum
from enums.config_enum import ConfigEnum
from tqdm import tqdm


def main():
    # Create a Harmonizer instance
    harmonizer = Harmonizer()

    # Extract data
    my_url = 'https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional/exportaciones'
    list_datainfo = harmonizer.extract(url=my_url, depth=0, ext=['.csv','.xls','.xlsx','.zip'])
    harmonizer = Harmonizer(list_datainfo)

    # Transform data
    harmonizer.transform()

    # Load the data
    results = harmonizer.load()

    # Print the results
    for i, result in enumerate(results):
        print(f"Dataset {i + 1}: Success: {result[0]}, Message: {result[1]}")
    

if __name__ == "__main__":
    main()
