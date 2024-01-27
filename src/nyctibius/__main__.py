import argparse
import logging
from .harmonizer import Harmonizer


def main(url, depth, ext):
    """
    Main function to extract, transform and load data.

    Parameters:
    url (str): The URL to extract data from.
    depth (int): The depth of the extraction.
    ext (list): The file extensions to consider during extraction.
    """
    try:
        # Create a Harmonizer instance
        harmonizer = Harmonizer()

        # Extract data
        list_datainfo = harmonizer.extract(url=url, depth=depth, ext=ext)
        harmonizer = Harmonizer(list_datainfo)

        # Transform data
        harmonizer.transform()

        # Load the data
        results = harmonizer.load()

        # Log the results
        for i, result in enumerate(results):
            logging.info(f"Dataset {i + 1}: Success: {result[0]}, Message: {result[1]}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Extract, transform and load data.')
    parser.add_argument('--url', type=str, required=True, help='The URL to extract data from.')
    parser.add_argument('--depth', type=int, default=0, help='The depth of the extraction.')
    parser.add_argument('--ext', nargs='+', default=['.csv', '.xls', '.xlsx', '.zip'], help='The file extensions to '
                                                                                            'consider during '
                                                                                            'extraction.')
    args = parser.parse_args()

    # Run the main function
    main(args.url, args.depth, args.ext)
