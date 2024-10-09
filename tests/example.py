from socio4health import Transformer
from socio4health import Extractor

# Define the parameters for the extractor
path = None
url = "https://microdatos.dane.gov.co/index.php/catalog/643/download/12161"
depth = 0
down_ext = ['.csv']
download_dir = "data/input"
output_path = "data/output"
key_words = []

# Create an instance of Extractor and perform extraction
extractor = Extractor(path=path, url=url, depth=depth, down_ext=down_ext, download_dir=download_dir,
                      key_words=key_words)
datainfo_list = extractor.extract()

for data_info in datainfo_list:
    transformer = Transformer(data_info=data_info, output_path=output_path)
    ac = transformer.available_columns()
    print(ac)

