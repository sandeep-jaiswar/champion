#!/usr/bin/env python3
from datetime import date
from champion.scrapers.nse.bhavcopy import BhavcopyScraper
from champion.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser

s = BhavcopyScraper()
# change this date as needed
target = date(2024,1,1)
print('Downloading for', target)
path = s.scrape(target, dry_run=False)
print('Downloaded to', path)

# If file exists, show first 20 lines
try:
    with open(path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i>=20:
                break
            print(line.strip())
except Exception as e:
    print('Failed to read file:', e)

# Try parsing raw CSV
p = PolarsBhavcopyParser()
try:
    df = p.parse_raw_csv(path)
    print('Parsed rows:', len(df))
    print('Columns:', df.columns)
except Exception as e:
    print('Parsing failed:', e)
