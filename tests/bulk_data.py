import os
import datetime
from zoneinfo import available_timezones

import pandas as pd

from socio4health import Extractor
from socio4health.enums.data_info_enum import BraColnamesEnum, BraColspecsEnum
from socio4health.harmonizer import Harmonizer
from socio4health.utils import harmonizer_utils
from socio4health.utils.extractor_utils import s4h_parse_fwf_dict
from socio4health.utils.harmonizer_utils import s4h_standardize_dict


if __name__ == "__main__":
    col_online_extractor = Extractor(
        input_path="https://microdatos.dane.gov.co/index.php/catalog/861/get-microdata",
        down_ext=['.csv', '.zip'],
        sep=';',
        output_path="data",
        depth=0
    )
    col_online_extractor.s4h_extract()