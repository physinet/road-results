import re
import json

from datetime import datetime
from requests_futures.sessions import FuturesSession
from bs4 import BeautifulSoup, SoupStrainer

from preprocess import clean

session = FuturesSession(max_workers=8)


def get_metadata(race_page_text):
    """Extract metadata from BikeReg race results page text,
    e.g. https://results.bikereg.com/race/11456
    """
    row = {}  # dictionary to keep track of values to add to table

    if 'No data' not in race_page_text:
        strain = SoupStrainer(['span'], {'class': 'downloadoptions'})
        soup = BeautifulSoup(race_page_text, features='lxml', parse_only=strain)
        row['json_url'] = soup.select('span.downloadoptions')[0] \
                              .find_all('a')[1]['href']

        strain = SoupStrainer(['div'], {'id': 'resultstitle'})
        soup = BeautifulSoup(race_page_text, features='lxml', parse_only=strain)
        resultstitle = soup.select('div#resultstitle')[0].text
        row['name'], date, *loc = [x.strip() for x in resultstitle.split('•')]
        # Sometimes location is not present, in which case loc will equal []

        # parse date
        regex = re.compile(r'([A-Za-z]{3})\s+(\d{1,2})\s+(\d{4})')
        row['date'] = datetime.strptime(regex.search(date).group(), '%b %d %Y')

        # parse location - always followed by random whitespace and some other
        # unneeded information
        if loc:
            row['loc'] = re.split('[\t\n\r]', loc[0])[0].strip()

        # coordinates (often not available)
        # search = re.search(r'GetMap\("([-\d\.]+):([-\d\.]+)"', race_page_text)
        # if search:
            # row['lat'], row['lng'] = map(float, search.groups())

    return row

def scrape_race_page(race_id):
    """Scrapes BikeReg race pages to extract race metadata for the race with
    given race_id. As of late 2020, there are no races with race_id > 13000.
    """
    future = session.get(f'https://results.bikereg.com/race/{race_id}')
    print(f'Scraping race with id {race_id}')
    if race_id in [12534, 12535]:  # these didn't work - ignoring
        return {}
    row = get_metadata(future.result().text)
    if row and row.get('json_url'):
        return dict(race_id=race_id, **row)
    return {}


def scrape_race_pages(race_ids=list(range(1, 13000))):
    """Scrapes all BikeReg race pages to extract metadata for list of race_ids.
    """
    print('Scraping race pages!')
    rows = [scrape_race_page(race_id) for race_id in race_ids]

    return rows


def scrape_results(json_urls):
    """Scrapes BikeReg results from list of JSON urls."""
    futures = [session.get(f'https://results.bikereg.com/{url}')
                            for url in json_urls]
    all_rows = []
    for url, future in zip(json_urls, futures):
        race_id = re.search('(\d+)', url).group()
        print(race_id)
        rows = json.loads(future.result().text)
        [row.update({'race_id': race_id}) for row in rows]
        rows = clean(rows)
        all_rows.extend(rows)

    return all_rows
