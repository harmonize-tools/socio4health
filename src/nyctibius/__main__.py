import argparse
from .harmonizer import Harmonizer
from .db.modifier import Modifier
from .bird_agent import BirdAgent


def main(mode:str, path:str, url:str, depth:int, down_ext:list, download_dir:str, key_words:str, func_name:None, *args):
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
        # Create a Harmonizer instance
        harmonizer = Harmonizer()

        # Extract data

        dict_datainfo = harmonizer.extract(path=path, url=url, depth=depth, down_ext=down_ext, download_dir=download_dir, key_words=key_words)
        harmonizer = Harmonizer(dict_datainfo)

        # Transform data
        harmonizer.transform(delete_files=True)

        # Load the data
        harmonizer.load(delete_db=True)

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

    elif mode == 'chat':
        agent = BirdAgent()
        breakpoint()


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Extract, transform and load data or run db functions.')
    parser.add_argument('mode', type=str, choices=['etl', 'query', 'chat'], help='The mode of operation.')
    parser.add_argument('--path', type=str, help='The folder path to extract data from.')
    parser.add_argument('--url', type=str, help='The URL to extract data from.')
    parser.add_argument('--depth', type=int, default=0, help='The depth of the extraction.')
    parser.add_argument('--download_dir', type=str, default="data/input", help='help="Specify the directory path where the downloaded files will be saved. Default is data/input')
    parser.add_argument('--ext', nargs='+', default=['.csv', '.xls', '.xlsx', '.zip'],
                        help='The file extensions to consider during extraction.')
    parser.add_argument('--key_words', nargs='+', default=[], help='Key words to extract files')
    parser.add_argument('--func', type=str, help='The function name to run.')
    parser.add_argument('--args', nargs='*', default=[], help='The arguments to the function.')

    args = parser.parse_args()

    # Run the main function
    main(args.mode, args.path, args.url, args.depth, args.ext, args.download_dir, args.key_words, args.func, *args.args)