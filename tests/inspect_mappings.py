from pathlib import Path
from socio4health.utils.mapping_utils import load_mapping_bundle

print('Loading rd_mappings bundle via socio4health.utils.mapping_utils')
_DATA_DIR = Path(__file__).resolve().parent / "rd_mappings"
_BUNDLE = load_mapping_bundle(_DATA_DIR, value_mapping_prefix="enhogar")

hm = _BUNDLE["harmonized_mapping"]
print('HARMONIZED_MAPPING type:', type(hm))
print('AREA sample keys types:', [type(k) for k in hm['AREA']['VALUES'].keys()][:5])
print('AREA sample items:', list(hm['AREA']['VALUES'].items())[:10])
