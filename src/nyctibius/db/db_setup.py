import sqlite3


class DatabaseSetup:
    """ Database Setup class
    """

    def __init__(self, db_file):
        self.db_file = db_file


if __name__ == '__main__':
    db_setup = DatabaseSetup('data/output/nyctibius.db')
    db_setup.create_default_census_schema_co()
