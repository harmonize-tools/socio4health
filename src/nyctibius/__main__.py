import argparse
import logging
from .harmonizer import Harmonizer
from .db.modifier import Modifier


def main(mode, path=None, url=None, depth=0, ext=None, func_name=None, *args):
    """
    Main function to extract, transform and load data or run db functions.

    Parameters:
    mode (str): The mode of operation ('etl' or 'query').
    url (str): The URL to extract data from.
    depth (int): The depth of the extraction.
    ext (list): The file extensions to consider during extraction.
    func_name (str): The function name to run (for 'query' mode).
    args (list): The arguments to the function (for 'query' mode).
    """
    if mode == 'etl':
        try:
            # Create a Harmonizer instance
            harmonizer = Harmonizer()

            # Extract data
            list_datainfo = harmonizer.extract(path=path, url=url, depth=depth, ext=ext)
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

    elif mode == 'query':
        modifier = Modifier()

        # Mapping of function names to functions
        func_mapping = {
            'get_tables': modifier.get_tables,
            'get_columns': modifier.get_columns,
            'rename_table': modifier.rename_table,
            'rename_column': modifier.rename_column,
            'rename_table_columns': modifier.rename_table_columns,
            'set_primary_key': modifier.set_primary_key,
            'set_foreign_key': modifier.set_foreign_key,
        }

        # Get the function from the mapping
        func = func_mapping.get(func_name)

        # Call the function with the provided arguments
        result = func(*args)

        print(result)


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Extract, transform and load data or run db functions.')
    parser.add_argument('mode', type=str, choices=['etl', 'query'], help='The mode of operation.')
    parser.add_argument('--path', type=str, help='The folder path to extract data from.')
    parser.add_argument('--url', type=str, help='The URL to extract data from.')
    parser.add_argument('--depth', type=int, default=0, help='The depth of the extraction.')
    parser.add_argument('--ext', nargs='+', default=['.csv', '.xls', '.xlsx', '.zip'],
                        help='The file extensions to consider during extraction.')
    parser.add_argument('--func', type=str, help='The function name to run.')
    parser.add_argument('--args', nargs='*', default=[], help='The arguments to the function.')

    args = parser.parse_args()

    # Run the main function
    main(args.mode, args.path, args.url, args.depth, args.ext, args.func, *args.args)