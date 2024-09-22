import os

def get_database_uri():
    return os.getenv("DATABASE_URI", "postgresql://postgres:8800bc84f23af727f4e9@localhost:3200/postgres")

