import pandas as pd

from nyctibius.etl.transformer import Transformer
from nyctibius.harmonizer import Harmonizer
from nyctibius.dto.dataset import Dataset
from nyctibius.enums.headers_enum import HeadersEnum
from nyctibius.enums.config_enum import ConfigEnum


def main():
    """
    # Load or create your list of datasets here
    data1 = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6]
    })
    dataset1 = Dataset(data1, 'My Dataset 1', 'This is my first dataset', 'https://example.com')

    data2 = pd.DataFrame({
        'C': [7, 8, 9],
        'D': [10, 11, 12]
    })
    dataset2 = Dataset(data2, 'My Dataset 2', 'This is my second dataset', 'https://example.com')

    datasets = [dataset1, dataset2]

    # Create a Harmonizer instance
    harmonizer = Harmonizer(datasets)

    """
    # Create a Harmonizer instance
    harmonizer = Harmonizer(None)

    csv = '../../data/input/Person.csv'
    # Transform the datasets
    harmonizer = harmonizer.transform('Person')

    # Load the data
    results = harmonizer.load()

    # Print the results
    for i, result in enumerate(results):
        print(f"Dataset {i + 1}: Success: {result[0]}, Message: {result[1]}")


if __name__ == "__main__":
    main()
