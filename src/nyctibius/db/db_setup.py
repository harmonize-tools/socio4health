import sqlite3


class DatabaseSetup:
    """ Database Setup class
    """

    def __init__(self, db_file):
        self.db_file = db_file

    def create_default_census_schema_co(self):
        """ Create necessary tables in the SQLite database """
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Dwelling (
                survey_id INTEGER PRIMARY KEY,
                type INTEGER,
                ethnic_territoriality INTEGER,
                ethnic_territoriality_type INTEGER,
                ethnic_territoriality_code INTEGER,
                protected_area INTEGER,
                protected_area_code INTEGER,
                use INTEGER,
                dwelling_type INTEGER,
                occupancy_status INTEGER,
                household_number INTEGER,
                wall_material INTEGER,
                floor_material INTEGER,
                electricity_service INTEGER,
                social_stratum INTEGER,
                aqueduct_service INTEGER,
                sewerage_service INTEGER,
                gas_service INTEGER,
                rubbish_service INTEGER,
                rubbish_service_per_week INTEGER,
                internet_service INTEGER,
                sanitary_service INTEGER,
                institution_type INTEGER,
                has_household INTEGER,
                total_residents INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Household (
                household_id INTEGER PRIMARY KEY,
                survey_id INTEGER,
                hosehold_in_dwelling INTEGER,
                room_number INTEGER,
                bedroom_number INTEGER,
                kitchen_type INTEGER,
                water_source INTEGER,
                desceased_number INTEGER,
                FOREIGN KEY (survey_id) REFERENCES Dwelling (survey_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Geroreference (
                survey_id INTEGER PRIMARY KEY,
                country_id INTEGER,
                department INTEGER,
                municipality INTEGER,
                class INTEGER,
                locality INTEGER,
                rural_sector INTEGER,
                rural_section INTEGER,
                populated_center INTEGER,
                household_number INTEGER,
                urban_sector INTEGER,
                urban_section INTEGER,
                block INTEGER,
                building_number INTEGER,
                housing_number INTEGER,
                FOREIGN KEY (survey_id) REFERENCES Dwelling (survey_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Person (
                person_id INTEGER PRIMARY KEY,
                survey_id INTEGER,
                hosehold_in_dwelling INTEGER,
                person_number INTEGER,
                sex INTEGER,
                age_group INTEGER,
                head_relationship INTEGER,
                ethnic_group INTEGER,
                indigenous_group INTEGER,
                occupancy_status INTEGER,
                clan INTEGER,
                vitsa INTEGER,
                kumpania INTEGER,
                speaks_native_language INTEGER,
                understands_native_language INTEGER,
                other_native_languages INTEGER,
                number_native_languages INTEGER,
                birthplace INTEGER,
                residence_five_years INTEGER,
                residence_one_year INTEGER,
                health_problem INTEGER,
                main_treatment INTEGER,
                cared_health_problem  INTEGER,
                heath_service_quality INTEGER,
                daily_life_difficulty INTEGER,
                read_write INTEGER,
                school_attendance INTEGER,
                education_level INTEGER,
                past_week_activities INTEGER,
                marital_status INTEGER,
                born_children  INTEGER,
                total_children INTEGER,
                male_children INTEGER,
                female_children INTEGER,
                surviving_children INTEGER,
                surviving_total INTEGER,
                surviving_male INTEGER,
                surviving_female INTEGER,
                outside_children INTEGER,
                outside_total INTEGER,
                outside_males INTEGER,
                outside_females INTEGER,
                last_child_born INTEGER,
                last_child_born_month INTEGER,
                last_child_born_year INTEGER,
                FOREIGN KEY (survey_id) REFERENCES Dwelling (survey_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Deceased (
                deceased_id INTEGER PRIMARY KEY,
                survey_id INTEGER,
                deceased_number INTEGER,
                household_in_dwelling INTEGER,
                sex INTEGER,
                age_death INTEGER,
                death_certificate INTEGER
            )
        ''')

        # Commit the changes and close the connection
        conn.commit()
        conn.close()


if __name__ == '__main__':
    db_setup = DatabaseSetup('data/output/nyctibius.db')
    db_setup.create_default_census_schema_co()
