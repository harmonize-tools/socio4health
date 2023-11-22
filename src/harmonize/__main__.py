from etl.extractor import Extractor

def main():
    extractor = Extractor()
    extractor.run_scrapy_spider()
    extractor.extract()


if __name__ == "__main__":
    main()
