from nyctibius.harmonizer import Harmonizer


def main():

    # Create a Harmonizer instance
    harmonizer = Harmonizer()

    # Extract data
    my_url = 'https://microdatos.dane.gov.co/index.php/catalog/643/get_microdata'
    list_datainfo = harmonizer.extract(url=my_url)
    harmonizer = Harmonizer(list_datainfo)

    # Transform data
    harmonizer.transform('Person')

    # Load the data
    results = harmonizer.load()

    # Print the results
    for i, result in enumerate(results):
        print(f"Dataset {i + 1}: Success: {result[0]}, Message: {result[1]}")


if __name__ == "__main__":
    main()
