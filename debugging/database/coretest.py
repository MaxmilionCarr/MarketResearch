from database.db import DataBase
import sqlite3 as sql
import os
import time
from dotenv import load_dotenv
import argparse

load_dotenv()
test_env_path = os.getenv("TESTING_DATABASE_PATH")
  

# --- Database Tests ---

# Pre Populate DB
def populate_exchanges(path = test_env_path):
    db = DataBase(path)
    exch_repo = db.exchange_repo

    exchanges = [
        ("NASDAQ", "America/New_York"),
        ("LSE", "Europe/London"),
        ("JPX", "Asia/Tokyo"),
        ("SSE", "Asia/Shanghai"),
    ]

    for name, timezone in exchanges:
        try:
            exch_repo.create(name, timezone)
            print(f"Created exchange: {name}")
        except sql.IntegrityError:
            print(f"Exchange {name} already exists.")

def populate_markets(path = test_env_path):
    db = DataBase(path)
    market_repo = db.market_repo
    exch_repo = db.exchange_repo

    # Fetch exchange IDs
    nasdaq_id = exch_repo.get_or_create("NASDAQ", timezone="America/New_York")

    markets = [
        (1, nasdaq_id),  # STOCKS on NASDAQ
    ]

    for market_id, exchange_id in markets:
        try:
            market_repo.create(market_id, exchange_id)
            print(f"Created market ID {market_id} for exchange ID {exchange_id}")
        except sql.IntegrityError:
            print(f"Market ID {market_id} for exchange ID {exchange_id} already exists.")

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
    exch_repo = db.exchange_repo

    checklist = {
        "Creating" : 0,
        "Reading" : 0,
        "Updating" : 0,
        "Deleting" : 0,
        "Links" : 0,
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

    # Testing NASDAQ link to markets
    print("Testing link between NYSE exchange and markets...")
    try:
        info = exch_repo.get_info(exch_repo.get_or_create("NASDAQ", timezone="America/New_York"))
        if info is not None:
            print(f"Exchange has {len(info.markets)} linked market(s).")
            checklist["Links"] = 1
            for market in info.markets:
                print(f"- Market ID: {market.market_id}, Market Name: {market.market_name}")
        else:
            print("No markets linked to this exchange.")
            checklist["Links"] = 0
    except Exception as e:
        checklist["Links"] = 0
        print("Failed to retrieve linked markets:", e)
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


# --- Cache Test ---


# --- Main ---
def main():
    parser = argparse.ArgumentParser(description="Run database tests.")
    parser.add_argument('--test', type=str, choices=['reset', 'all', 'database', 'exchanges', 'markets'], default='all',
                        help="Specify which tests to run: 'all', 'exchanges', or 'markets'. Default is 'all'.")
    args = parser.parse_args()

    test_map = {
        'reset': [test_database_delete, test_database_create, populate_exchanges, populate_markets],
        'all': [test_database_create, test_exchanges],
        'database': [test_database_create],
        'exchanges': [test_exchanges],
        # 'markets': [test_markets],  # Uncomment when market tests are implemented
    }

    tests_to_run = test_map.get(args.test, [])
    for test in tests_to_run:
        print(f"Running {test.__name__}...")
        test()
        print(f"Finished {test.__name__}.\n")


if __name__ == "__main__":
    main()