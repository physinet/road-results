import re
import json

from datetime import datetime
from requests_futures.sessions import FuturesSession

from preprocess import clean

# Finds the appropriate HTML tag and following text
metadata_regex = re.compile(r'resultstitle" >(.*?)[\n\r]')
# e.g. Jan 3 2001
date_regex = re.compile(r'([A-Za-z]{3})\s+(\d{1,2})\s+(\d{4})')

BAD_IDS = [12534, 12535]  # these didn't work - ignoring


def get_futures(race_ids=list(range(1, 13000))):
    """Get Futures for all BikeReg race pages with given race_ids."""
    session = FuturesSession(max_workers=8)
    return [session.get(f'https://results.bikereg.com/race/{race_id}')
                for race_id in race_ids if race_id not in BAD_IDS]

def get_results_futures(urls):
    """Get Futures for all BikeReg JSON pages with given json_urls."""
    session = FuturesSession(max_workers=8)
    return [session.get(f'https://results.bikereg.com/{url}') for url in urls]

def get_metadata(race_page_text):
    """Extract metadata from BikeReg race results page text,
    e.g. https://results.bikereg.com/race/11456
    """
    row = {}  # dictionary to keep track of values to add to table

    if 'No data' not in race_page_text:
        group = metadata_regex.search(race_page_text).groups()[0]
        row['name'], date, *loc = [x.strip() for x in group.split('&bull;')]
        # Sometimes location is not present, in which case loc will equal []

        row['date'] = parse_date(date)

        # parse location - always followed by random whitespace and some other
        # unneeded information
        if loc:
            row['loc'] = re.split('[\t\n\r]', loc[0])[0].strip()

    return row


def parse_date(date):
    """Converts a date string to datetime object."""
    return datetime.strptime(date_regex.search(date).group(), '%b %d %Y')


def scrape_race_page(race_id, text):
    """Scrapes BikeReg race pages to extract race metadata for the race with
    given race_id. As of late 2020, there are no races with race_id > 13000.
    """
    print(f'Scraping race with id {race_id}')

    row = get_metadata(text)
    if row:
        return dict(race_id=race_id,
                    json_url=f'downloadrace.php?raceID={race_id}&json=1',
                    **row)
    return {}


def scrape_results_json(race_id, text):
    """Scrapes BikeReg results from JSON."""
    rows = json.loads(text)
    [row.update({'race_id': race_id}) for row in rows]
    return clean(rows)
