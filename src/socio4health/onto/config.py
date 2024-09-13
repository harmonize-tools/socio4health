# Define the option mapping JSON
import json

option_mapping_file = 'options_mapping.json'

with open(option_mapping_file, 'r') as file:
    option_mapping = json.load(file)


def get_general_option(local_var_name, local_option, option_mapping):
    if local_var_name in option_mapping["school_education"]:
        local_var_data = option_mapping["school_education"][local_var_name]
        if str(local_option) in local_var_data["mapping_to_detailed"]:
            detailed_option = local_var_data["mapping_to_detailed"][str(local_option)]

            # Now, find the general option for the detailed option
            for general_option, data in option_mapping["school_education"]["option"].items():
                if detailed_option in data["detailed"]:
                    return general_option, data["label"]

    return None, "No match found"


# Example usage
local_var_name = "V0628"
local_option = 3

general_option, label = get_general_option(local_var_name, local_option, option_mapping)
print(f"General Option: {general_option}, Label: {label}")