from nyctibius.harmonizer import Harmonizer
from nyctibius.dto.data_info import DataInfo


def main():
    try:
        # Define your datasets here
        datasets = [
            DataInfo(file_path="path_to_file1", description="description1", url="url1"),
            DataInfo(file_path="path_to_file2", description="description2", url="url2")
        ]

        # Create an instance of the Harmonizer class
        harmonizer = Harmonizer(datasets)

        # Extract data
        harmonizer.extract(urls=["url1", "url2"])

        # Transform data
        harmonizer.transform(table_name="table_name", headers=None)

        # Load data
        results = harmonizer.load()

        # Print results
        for result in results:
            print(result)

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()