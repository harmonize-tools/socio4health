from socio4health import DataInfo
import pandas as pd

from socio4health.enums.data_info_enum import CountryEnum, DataSourceTypeEnum


def read_csv_dict(file_path: str) -> pd.DataFrame:
    """
    Reads a CSV file and returns a DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        DataFrame: The DataFrame containing the CSV data.
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        raise ValueError(f"Error reading CSV file: {str(e)}")


class Translator:
    def __init__(self, data_info: DataInfo):
        self.dictionary = None
        self.data_info = data_info

    def select_dictionary(self, dictionary):
        """
        Selects the dictionary to use for translation.

        Args:
            dictionary (str): CSV file with the dictionary to use for translation.
        """
        if self.data_info is None:
            raise ValueError("DataInfo object must be provided to select dictionary.")

        if dictionary is None:
            if self.data_info.country is CountryEnum.COLOMBIA and self.data_info.year is 2018 and self.data_info.data_source_type is DataSourceTypeEnum.CENSUS:
                dictionary = read_csv_dict('files/COL2018.csv')
            elif self.data_info.country is CountryEnum.BRAZIL and self.data_info.year is 2010 and self.data_info.data_source_type is DataSourceTypeEnum.CENSUS:
                dictionary = read_csv_dict('files/BRA2010.csv')
            elif self.data_info.country is CountryEnum.PERU and self.data_info.year is 2017 and self.data_info.data_source_type is DataSourceTypeEnum.CENSUS:
                dictionary = read_csv_dict('files/PER2017.csv')
            else:
                raise ValueError("Dictionary not found for the specified country, year, and data source type.")

        self.dictionary = dictionary

    def translate(self, df_data, dictionary) -> pd.DataFrame:
        #continuar


