import requests
import csv

BASE_URL = "https://data.wprdc.org/api/3/action/datastore_search?resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1&q="
OUTPUT_FILE = "output.csv"

pids = [
'0088M00076000000',
'0177B00188000000',
'0177F00162000000',
'0177A00203000000',
'0177A00159000000',
'0177E00246000000',
'0177F00314000000',
'0177F00182000000',
'0177E00212000000',
'0176N00083000000',
'0177B00200000000',
'0177E00184000000',
'0177A00178000000',
'0177E00290000000',
'0176N00074000000',
'0177F00304000000',
'0177F00271000000',
'0177K00052000000',
'0177A00205000000',
'0177A00127000000',
'0088H00150000000',
'0177F00174000000',
'0177B00218000000',
'0177B00212000000',
'0177B00208000000',
'0177B00180000000',
'0177A00001000000',
'0177E00195000000',
'0177A00086000000',
'0177B00190000000',
'0177A00011000000',
'0176N00080000000',
'0176N00079000000',
'0177A00082000000',
'0177F00258000000',
'0177K00042000000',
'0177J00069000000',
'0177K00068000000',
'0177K00066000000',
'0177K00058000000',
'0177F00188000000',
'0177K00078000000',
'0177F00340000000',
'0177F00336000000',
'0177F00278000000',
'0177F00231000000',
'0177F00224000000',
'0177F00160000000',
'0177E00126000000',
'0177J00020000000',
'0177E00060000000',
'0177E00112000000',
'0177E00100000000',
'0176N00065000000',
'0177E00222000000',
'0177E00092000000',
'0176N00219000000',
'0176N00206000000',
'0177E00163000000',
'0177E00156000000',
'0177E00148000000',
'0177E00140000000',
'0176N00189000000',
'0177A00140000000',
'0177A00125000000',
'0177A00123000000',
'0177A00121000000',
'0177A00242000000',
'0177A00210000000',
'0177A00201000000',
'0177A00194000000',
'0177A00192000000',
'0177A00173000000',
'0177A00163000000',
'0088M00102000000',
'0088H00186000000',
'0088H00183000000',
'0088M00006000000',
'0088H00153000000',
'0088H00152000000'
]

def fetch_data(item_id):
    response = requests.get(f"{BASE_URL}{item_id}")
    response.raise_for_status()
    return response.json()


def main():
    all_records = []
    for item_id in pids:
        try:
            data = fetch_data(item_id)
            records = data.get("result", {}).get("records", [])
            all_records.extend(records)
        except Exception as e:
            print(f"Error fetching {item_id}: {e}")

    if not all_records:
        print("No data fetched — check your IDs or API URL.")
        return

    # Collect all unique field names across records
    fieldnames = set()
    for record in all_records:
        fieldnames.update(record.keys())
    fieldnames = sorted(fieldnames)  # optional: keep consistent order

    # Write to CSV
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_records)

    print(f"Saved {len(all_records)} records to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()