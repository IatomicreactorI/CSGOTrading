import requests
import pandas as pd
import datetime
import time
import random
import os

candidate_items = [
    'M4A4 | 龍王 (Dragon King) (Factory New)',
    'Operation Broken Fang Case',
    'Desert Eagle | Mecha Industries (Factory New)',
    'M4A4 | Neo-Noir (Factory New)',
    'Operation Wildfire Case',
    'Desert Eagle | Printstream (Factory New)',
    'Sticker | Bolt Energy (Foil)',
    'M4A1-S | Leaded Glass (Factory New)',
    'Operation Riptide Case',
    'Sticker | Hypnoteyes (Holo)',
    'Glove Case',
    'Sticker | Taste Buddy (Holo)',
    'AK-47 | Bloodsport (Factory New)',
    'AWP | Hyper Beast (Factory New)',
    'M4A4 | Desolate Space (Factory New)',
    'Dreams & Nightmares Case',
    'Sticker | FaZe Clan (Holo) | Paris 2023',
    'M4A1-S | Decimator (Factory New)',
    'Sticker | Team Liquid (Holo) | Paris 2023',
    'AK-47 | Asiimov (Factory New)',
]


ua_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
]

def get_price_info(market_hash_name):
    url = 'https://steamcommunity.com/market/priceoverview/'
    params = {
        'country': 'US',
        'currency': 1,
        'appid': 730,
        'market_hash_name': market_hash_name
    }
    headers = {
        'User-Agent': random.choice(ua_list)
    }
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=20)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"status code returned: {resp.status_code}")
            return None
    except Exception as e:
        print(f"error returned: {e}")
        return None

csv_file = os.path.join(os.path.dirname(__file__), "cs2_data.csv")

# 1. read historical data, build latest data dictionary
try:
    old = pd.read_csv(csv_file)
    # only keep the latest one for each item
    latest_data = old.sort_values("batch_id").groupby("name").tail(1).set_index("name").to_dict("index")
except FileNotFoundError:
    latest_data = {}

records = []
failed_items = []

# batch information
batch_id = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"\n=== batch {batch_id} started ===")
print(f"target item amount: {len(candidate_items)}")
print("=" * 50)

def clean_price(p):
    if not p or p == 'none':
        return None
    if isinstance(p, str):
        return float(p.replace('$', '').replace(',', '').strip())
    return p

success_count = 0

random.shuffle(candidate_items)  # shuffle before each collection
for name in candidate_items:
    info = get_price_info(name)
    url_name = name.replace(' ', '%20')
    item_url = f"https://steamcommunity.com/market/listings/730/{url_name}"
    if info and info.get('success'):
        lowest = info.get('lowest_price', 'none')
        median = info.get('median_price', 'none')
        volume = info.get('volume', 'none')
        open_price = clean_price(lowest)
        close_price = clean_price(median)
        if open_price is None or close_price is None:
            print(f"[--/{len(candidate_items)}] invalid price, skip: {name}\n   item page: {item_url}")
            failed_items.append(name)
            continue
        try:
            volume = int(volume.replace(',', ''))
        except:
            volume = 0
        records.append({
            "name": name,
            "batch_id": batch_id,
            "open": open_price,
            "close": close_price,
            "volume": volume,
            "item_url": item_url
        })
        success_count += 1
        print(f"[batch {batch_id}] [{success_count}/{len(candidate_items)}] collection success: {name}\n   item page: {item_url}")
    else:
        print(f"[--/{len(candidate_items)}] no data obtained: {name}\n   item page: {item_url}")
        failed_items.append(name)
    time.sleep(random.uniform(4, 10))

# failed to collect (maximum 3 retries)
for retry in range(3):
    if not failed_items:
        break
    print(f"start {retry+1}th failed collection, remaining {len(failed_items)} items")
    to_retry = failed_items.copy()
    failed_items = []
    random.shuffle(to_retry)  # shuffle before each supplement collection
    supplement_success_count = 0
    for name in to_retry:
        info = get_price_info(name)
        url_name = name.replace(' ', '%20')
        item_url = f"https://steamcommunity.com/market/listings/730/{url_name}"
        if info and info.get('success'):
            lowest = info.get('lowest_price', 'none')
            median = info.get('median_price', 'none')
            volume = info.get('volume', 'none')
            open_price = clean_price(lowest)
            close_price = clean_price(median)
            if open_price is None or close_price is None:
                print(f"[supplement collection --/{len(to_retry)}] invalid price, skip: {name}\n   item page: {item_url}")
                failed_items.append(name)
                continue
            try:
                volume = int(volume.replace(',', ''))
            except:
                volume = 0
            records.append({
                "name": name,
                "batch_id": batch_id,
                "open": open_price,
                "close": close_price,
                "volume": volume,
                "item_url": item_url
            })
            supplement_success_count += 1
            print(f"[batch {batch_id}] [supplement collection {supplement_success_count}/{len(to_retry)}] supplement collection success: {name}\n   item page: {item_url}")
        else:
            print(f"[supplement collection --/{len(to_retry)}] supplement collection no data obtained: {name}\n   item page: {item_url}")
            failed_items.append(name)
        time.sleep(random.uniform(4, 10))

# if failed to collect, supplement logic
for name in failed_items:
    if name in latest_data:
        last = latest_data[name]
        print(f"use historical data to supplement: {name}")
        records.append({
            "name": name,
            "batch_id": batch_id,
            "open": last["open"],
            "close": last["close"],
            "volume": last["volume"],
            "item_url": last["item_url"]
        })
    else:
        print(f"first collection and multiple failed, continue supplement collection: {name}")

# save to CSV, append mode
df = pd.DataFrame(records)
try:
    old = pd.read_csv(csv_file)
    df = pd.concat([old, df], ignore_index=True)
except FileNotFoundError:
    pass
df.to_csv(csv_file, index=False, encoding='utf-8-sig')
print(f"saved {len(records)} records to {csv_file}")

# write failed items to file
if failed_items:
    with open('failed_items.txt', 'w', encoding='utf-8') as f:
        for name in failed_items:
            f.write(name + '\n')
    print(f"finally {len(failed_items)} items failed to collect, written to failed_items.txt")
else:
    print("all items collected successfully!")

print(f"\n=== batch {batch_id} completed ===")
print(f"success collection: {len(records)} items")
print(f"failed collection: {len(failed_items)} items")
print("=" * 50)