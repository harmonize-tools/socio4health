from enum import Enum

class ColumnMappingEnum(Enum):
    GEIH = {
        'P3271': 'sex',
        'P6050': 'relationship',
        'P6070': 'marital'
    }

class ValueMappingEnum(Enum):
    GEIH = {
        'sex': {'1': 'male', '2': 'female'},
        'relationship': {
            '1': 'head',
            '2': 'partner',
            '3': 'child',
            '4': 'grandchild',
            '5': 'other relative',
            '6': 'employee',
            '7': 'Retiree',
            '8': 'worker',
            '9': 'other non-relative'
        },

    }
