#!/bin/bash

# Outputs vars

now="$(date +'%Y-%m-%d_%H-%M-%S')"

STDOUT="data" 
FILENAME="${now}_items.jsonl" 

# Spiders vars
SCRAPER_PATH="scraper/spiders"
AMAZON_SPIDER="amazon.py" 

echo "Activating venv" 
source .venv/Scripts/activate

echo "Running Scraper for Amazon BlackFriday products!" 
cd src/scraper
scrapy crawl amazon -o "$STDOUT/$FILENAME" 

echo "Running ETL" 
cd ../.. 
python -m src.etl.pipeline