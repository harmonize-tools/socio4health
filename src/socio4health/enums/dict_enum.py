from enum import Enum


class MappingHeaderEnum(Enum):
    VARIABLE = 'variable'
    CODE = 'code'
    HARMONIZED_VARIABLE = 'har_variable'
    HARMONIZED_CODE = 'har_code'
    HARMONIZED_LABEL = 'har_label'
    DETAILED_CODE = 'd_code'
    DETAILED_LABEL = 'd_label'
    VARIABLE_TYPE = 'variable_type'
    RANGE = 'range'
    NULL_VALUE = 'null_val'
