from enum import Enum


class CountryEnum(Enum):
    COLOMBIA = 'COL'
    PERU = 'PER'
    BRAZIL = 'BRA'
    DOMINICAN_REPUBLIC = 'DOM'


class DataSourceTypeEnum(Enum):
    CENSUS = 'CENSUS'
    SURVEY = 'SURVEY'
