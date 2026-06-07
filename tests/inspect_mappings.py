import sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))

print('sys.path[0]', sys.path[0])

try:
    import rd_year_mappings as r
    hm = r.HARMONIZED_MAPPING
    print('rd_year_mappings HARMONIZED_MAPPING type:', type(hm))
    print('AREA sample keys types:', [type(k) for k in hm['AREA']['VALUES'].keys()][:5])
    print('AREA sample items:', list(hm['AREA']['VALUES'].items())[:10])
except Exception as e:
    print('rd_year_mappings import failed', e)

try:
    import tests.rd_year_mappings as trm
    hm2 = trm.HARMONIZED_MAPPING
    print('tests.rd_year_mappings HARMONIZED_MAPPING type:', type(hm2))
    print('AREA sample keys types:', [type(k) for k in hm2['AREA']['VALUES'].keys()][:5])
    print('AREA sample items:', list(hm2['AREA']['VALUES'].items())[:10])
except Exception as e:
    print('tests.rd_year_mappings import failed', e)
