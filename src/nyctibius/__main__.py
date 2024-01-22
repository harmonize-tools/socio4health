import pandas as pd

from harmonizer import Harmonizer


def main():

    # Create a Harmonizer instance
    harmonizer = Harmonizer()

    # Extract data
    #my_url = "https://www.dane.gov.co/index.php/estadisticas-por-tema"
    my_url = "https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional/exportaciones"

    list_datainfo = harmonizer.extract(url=my_url, depth = 0)
    print(list_datainfo)
    """
    harmonizer = Harmonizer(list_datainfo)
    # Transform data
    harmonizer.transform('Person')

    # Load the data
    results = harmonizer.load()

    # Print the results
    for i, result in enumerate(results):
        print(f"Dataset {i + 1}: Success: {result[0]}, Message: {result[1]}")
    """

if __name__ == "__main__":
    main()
