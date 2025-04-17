import requests
import csv
import time
from datetime import datetime
import os

# Set the CSV file name and headers
csv_filename = 'sale_data_all_items.csv'
csv_headers = ['timestamp', 'item_name', 'sale_price', 'prev_price', 'item_id']

# Load existing entries from CSV to avoid duplicates
def load_existing_entries():
    existing_entries = set()
    if os.path.exists(csv_filename):
        with open(csv_filename, mode='r', newline='', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader, None)  # Skip header
            for row in csv_reader:
                # Create a unique key for each row
                key = (row[0], row[1], row[2], row[4])  # timestamp, item_name, sale_price, item_id
                existing_entries.add(key)
    return existing_entries

# Function to fetch sale activities
def fetch_sales_data():
    total_activities = []

    for page in range(1, 3):  # Fetch from multiple pages if needed
        print(f"Fetching sale activities from page {page}...")
        activity_res = requests.get(f"https://api.rolimons.com/market/v1/saleactivity?page={page}")

        if activity_res.status_code == 429:
            print("Rate limit reached! Sleeping for 45 seconds...")
            time.sleep(45)
            break

        if activity_res.status_code == 200:
            activities = activity_res.json().get("activities", [])
            if activities:
                total_activities.extend(activities)
                if len(total_activities) >= 70:
                    break
            else:
                print("No more sale activities found.")
                break
        else:
            print(f"Failed to fetch sale activities: {activity_res.status_code}")
            break

    return total_activities

# Function to get item names
def get_item_names():
    while True:
        items_res = requests.get("https://www.rolimons.com/itemapi/itemdetails")
        if items_res.status_code == 429:
            print("Rate limit reached for item details! Sleeping for 30 seconds...")
            time.sleep(30)
            continue
        if items_res.status_code == 200:
            return items_res.json().get("items", {})
        else:
            print(f"Failed to fetch item details: {items_res.status_code}")
            return None

# Function to write new, non-duplicate data to CSV
def write_to_csv(total_activities, item_names, existing_entries):
    with open(csv_filename, mode='a', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)

        if csv_file.tell() == 0:
            csv_writer.writerow(csv_headers)

        for sale in total_activities:
            timestamp, item_id, price, prev_price, serial_id = sale
            item_name = item_names.get(str(item_id), ["Unknown Item"])[0]
            time_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            # Create a unique key
            entry_key = (time_str, item_name, str(price), str(item_id))

            if entry_key not in existing_entries:
                csv_writer.writerow([time_str, item_name, price, prev_price, item_id])
                existing_entries.add(entry_key)
                print(f"Added to CSV: {time_str}, {item_name}, {price}, {prev_price}, {item_id}")
            else:
                print(f"Duplicate skipped: {time_str}, {item_name}, {price}, {item_id}")

# Main function
def run_continuous_collection():
    existing_entries = load_existing_entries()

    while True:
        print("Starting data collection...")
        activities = fetch_sales_data()

        if activities:
            item_names = get_item_names()
            if item_names:
                write_to_csv(activities, item_names, existing_entries)

        print("Waiting for 60 seconds before the next fetch...")
        time.sleep(60)

# Start
run_continuous_collection()
