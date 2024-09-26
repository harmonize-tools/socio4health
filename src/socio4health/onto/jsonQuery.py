import json


def query_json(json_file_path, variable, option, data_source):
    # Load the JSON data from the file
    with open(json_file_path, 'r', encoding='utf-8') as jsonfile:
        data = json.load(jsonfile)

    result = []

    # Check if the variable exists in the options
    if variable in data:
        options = data[variable].get("options", {})

        # Check if the option exists
        if option in options:
            detailed_mapping = options[option].get("detailed", {})
            print(detailed_mapping)

            # Check if the data source exists
            if data_source in data[variable].get("data_sources", {}):
                variables = data[variable]["data_sources"][data_source].get("variables", {})

                for var, var_data in variables.items():
                    mappings = var_data.get("mappings", {})

                    # Add mappings to the result
                    for code, mapping in mappings.items():
                        map_to = mapping.get("map_to")
                        map_label = detailed_mapping.get(map_to)
                        if map_label:
                            result.append({
                                "code": code,
                                "label": mapping.get("label"),
                                "mapped_code": map_to,
                                "mapped_label": map_label
                            })

    return result


# Example Usage
result = query_json('output.json', 'area_class', '2', 'COC2018')
for item in result:
    print(item)
