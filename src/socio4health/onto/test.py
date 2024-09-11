import pandas as pd


def get_harmonized_code(csv_file, harmonize_var, survey, code):
    # Load the CSV file into a DataFrame
    df = pd.read_csv(csv_file)

    # Identify the column name for the specified survey and harmonize variable
    survey_column = survey
    if survey_column not in df.columns:
        raise ValueError(
            f"Survey column '{survey_column}' does not exist in the CSV for harmonize variable '{harmonize_var}'.")

    # Drop rows where the survey column has NaN values
    df = df.dropna(subset=[survey_column])

    # Ensure the survey column is treated as a string
    df[survey_column] = df[survey_column].astype(str)

    # Filter the DataFrame based on the specified survey column and code
    row = df[df[survey_column].str.startswith(f'{code}=')]

    # Function to get the harmonized code based on harmonize_var
    def get_harmonized_code_from_row(row, harmonize_var):
        if harmonize_var in row:
            return row[harmonize_var].values[0]
        else:
            raise ValueError(f"Unknown harmonize variable '{harmonize_var}'.")

    # Check if the row exists and get the harmonized code
    if not row.empty:
        return get_harmonized_code_from_row(row, harmonize_var)
    else:
        return None


# Example usage:
csv_file = 'test.csv'
harmonize_var = 'AREA_CLASS'
survey = 'BR12010'
code = '4'

harmonized_code = get_harmonized_code(csv_file, harmonize_var, survey, code)
print(
    f'The harmonized code for survey {survey}, harmonize variable {harmonize_var}, and code {code} is: {harmonized_code}')
