import csv
import json


def csv_to_json(csv_file_path, json_file_path):
    data = {}

    with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            data_source = row['Data Source']
            variable = row['Variable']
            code = row['Code']
            label = row['Label']
            mapped_variable = row['Mapped Variable']
            mapped_code = row['Mapped Code']
            mapped_label = row['Mapped Label']
            mapped_detailed_code = row['Mapped Detailed Code']
            mapped_detailed_label = row['Mapped Detailed Label']

            # Initialize area_class options and detailed mapping if needed
            if mapped_variable not in data:
                data[mapped_variable] = {
                    "options": {},
                    "data_sources": {}
                }

            if mapped_code not in data[mapped_variable]["options"]:
                data[mapped_variable]["options"][mapped_code] = {
                    "label": mapped_label,
                    "detailed": {}
                }

            if mapped_detailed_code:
                if mapped_detailed_code not in data[mapped_variable]["options"][mapped_code]["detailed"]:
                    data[mapped_variable]["options"][mapped_code]["detailed"][mapped_detailed_code] = mapped_detailed_label

            # Initialize data_sources for this variable if needed
            if data_source not in data[mapped_variable]["data_sources"]:
                data[mapped_variable]["data_sources"][data_source] = {"variables": {}}

            if variable not in data[mapped_variable]["data_sources"][data_source]["variables"]:
                data[mapped_variable]["data_sources"][data_source]["variables"][variable] = {"mappings": {}}

            data[mapped_variable]["data_sources"][data_source]["variables"][variable]["mappings"][code] = {
                "label": label,
                "map_to": mapped_detailed_code
            }

    with open(json_file_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, indent=4, ensure_ascii=False)


# Usage
csv_to_json('mapping.csv', 'output.json')