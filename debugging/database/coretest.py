from database.db import DataBase
from database.core import exchanges, markets
from database.instruments import tickers
from database.technical_data import historical_prices
import sqlite3 as sql
import os
import time
from dotenv import load_dotenv

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")

# --- Database Tests ---

# Works
def test_database_create(path = test_env_path):
    db = DataBase(path)
    db.create_db()

# Works
def test_database_delete(path = test_env_path):
    db = DataBase(path)
    db.delete_db()

# --- Exchange Tests ---
def test_exchanges(path = test_env_path):
    db = DataBase(path)
    exch_repo = exchanges.ExchangeRepository(db.connection)

    checklist = {
        "Creating" : 0,
        "Reading" : 0,
        "Updating" : 0,
        "Deleting" : 0
    }
    
    print("---- EXCHANGE TESTS ----")
    # Creating and retrieving an exchange
    print("Fetching or creating NYSE exchange...")
    check = exch_repo.get_or_create("NYSE", timezone="America/New_York")
    if int(check):
        print("Exchange at ID", check, "exists or was created.")
        checklist["Creating"] = 1
        checklist["Reading"] = 1
    else:
        print("Failed to create or fetch exchange.")
        checklist["Creating"] = 0
        checklist["Reading"] = 0
    print()

    # Attempting to create a duplicate exchange
    print("Attempting to create duplicate NYSE exchange...")
    try:
        exch_repo.create("NYSE", "America/New_York")
        checklist["Creating"] = 0
        print("Duplicate exchange created, error in uniqueness constraint.")
    except sql.IntegrityError:
        print("Exchange already exists.")
    print()
    
    # Reading exchange data
    print("Retrieving exchange information...")
    info = exch_repo.get_info(check)
    if info and info.exchange_name == "NYSE":
        checklist["Reading"] = 1
        print("Exchange ID:", info.exchange_id)
        print("Exchange Name:", info.exchange_name)
        print("Timezone:", info.timezone)
        print("Exchange information retrieved successfully.")
    else:
        checklist["Reading"] = 0
        print("Failed to retrieve correct exchange information.")
    print()

    # Updating an exchange
    print("Updating NYSE exchange timezone...")
    try:
        id = info.exchange_id
        exch_repo.update(id, timezone="America/Los_Angeles")
        updated_info = exch_repo.get_info(check)
        print("Previous Timezone:", info.timezone)
        print("Updated Timezone:", updated_info.timezone)
        if updated_info.timezone == "America/Los_Angeles":
            checklist["Updating"] = 1
            print("Exchange updated successfully.")
        else:
            checklist["Updating"] = 0
            print("Exchange update failed.")
    except Exception as e:
        checklist["Updating"] = 0
        print("Failed to update exchange:", e)
    print()

    # Deleting an exchange
    print("Attempting to delete NYSE exchange...")
    try:
        exch_repo.delete(exchange_name="NYSE")
        checklist["Deleting"] = 1
        print("Exchange deleted successfully.")
    except Exception as e:
        checklist["Deleting"] = 0
        print("Failed to delete exchange:", e)
    print()
    
    # Confirming deletion
    print("Confirming deletion...")
    time.sleep(1)  # Ensure DB has processed deletion
    deleted_info = exch_repo.get_info(check)

    if deleted_info is None:
        print("Exchange successfully deleted.")
    else:
        print("Failed to delete exchange.")
        checklist["Deleting"] = 0

    print()
    print("Exchange Test Summary:")
    for k, v in checklist.items():
        status = "Passed" if v else "Failed"
        print(f"{k}: {status}")

# --- Market Tests ---


if __name__ == "__main__":
    test_exchanges()