import sqlite3

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('onto.sqlite3')
cursor = conn.cursor()

# Create the source_sdd table for sociodemographic data sources
cursor.execute('''
CREATE TABLE IF NOT EXISTS source_sdd (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country TEXT NOT NULL,
    source_sdd_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    organization TEXT,
    source_url TEXT
)
''')

# Create the harmonized_code table
cursor.execute('''
CREATE TABLE IF NOT EXISTS harmonized_code (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    har_var_name TEXT NOT NULL,
    har_code INTEGER NOT NULL,
    label TEXT NOT NULL
)
''')

# Create the mapping_option table
cursor.execute('''
CREATE TABLE IF NOT EXISTS mapping_option (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    harmonized_code INTEGER NOT NULL,
    source_sdd TEXT NOT NULL,
    local_var_name TEXT NOT NULL,
    option_code TEXT NOT NULL,
    option_label TEXT NOT NULL,
    FOREIGN KEY (harmonized_code) REFERENCES harmonized_code(id)
)
''')


# Function to insert a new SSDD source
def insert_source_sdd(country, source_sdd_name, year, organization, source_url):
    cursor.execute('''
    INSERT INTO source_sdd (country, source_sdd_name, year, organization, source_url)
    VALUES (?, ?, ?, ?, ?)
    ''', (country, source_sdd_name, year, organization, source_url))
    conn.commit()
    print(f"Inserted new Socio-Demographic Data Source: {source_sdd_name} for {country} ({year})")


# Function to insert a new harmonized code
def insert_harmonized_code(har_var_name, har_code, label):
    cursor.execute('''
    INSERT INTO harmonized_code (har_var_name, har_code, label)
    VALUES (?, ?, ?)
    ''', (har_var_name, har_code, label))
    conn.commit()
    print(f"Inserted new harmonized code: {har_code} ({label}) for {har_var_name}")


# Function to insert a new mapping option
def insert_mapping_option(harmonized_code, source_sdd, local_var_name, option_code, option_label):
    cursor.execute('''
    INSERT INTO mapping_option (harmonized_code, source_sdd, local_var_name, option_code, option_label)
    VALUES (?, ?, ?, ?, ?)
    ''', (harmonized_code, source_sdd, local_var_name, option_code, option_label))
    conn.commit()
    print(f"Inserted new mapping option: {option_code} ({option_label}) for {source_sdd}")


# Example usage of the functions
if __name__ == '__main__':
    # Insert a new SSDD source
    insert_source_sdd('Colombia', 'Census', 2018, 'DANE', 'https://microdatos.dane.gov.co/index.php/catalog/643/get_microdata')

    # Insert a new harmonized code
    insert_harmonized_code('AREA_CLASS', 10, 'Urban')

    # Insert a new mapping option
    insert_mapping_option(1, 'CO12018', 'UA_CLASE', '1', 'Cabecera Municipal')

# Close the database connection
conn.close()
