import re
from datetime import datetime
from requests_futures.sessions import FuturesSession
from bs4 import BeautifulSoup

from model import Races, Racers, Results

def get_metadata(race_page_text):
    """Extract metadata from BikeReg race results page text,
    e.g. https://results.bikereg.com/race/11456
    """
    row = {}  # dictionary to keep track of values to add to table

    if 'No data' not in race_page_text:
        soup = BeautifulSoup(race_page_text, features='lxml')
        row['json_url'] = soup.select('span.downloadoptions')[0] \
                              .find_all('a')[1]['href']

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
        search = re.search(r'GetMap\("([-\d\.]+):([-\d\.]+)"', race_page_text)
        if search:
            row['lat'], row['lng'] = map(float, search.groups())

    return row


def scrape_race_pages(race_ids=list(range(1, 13000))):
    """Scrapes BikeReg race pages to extract race metadata. race_ids is a
    list of the race_ids associated with the pages we want to scrape. As of
    late 2020, there are no races with ids greater than 13000.
    """
    session = FuturesSession(max_workers=8)
    futures = [session.get(f'https://results.bikereg.com/race/{i}') for i in race_ids]
    print('Scraping race pages!')
    rows = [{} for _ in race_ids]
    for i, (race_id, future) in enumerate(zip(race_ids, futures)):
        print(f'Scraping race with id {race_id}')
        if race_id in [12533, 12534]:  # these didn't work - ignoring
            continue
        rows[i] = dict(race_id=race_id, **get_metadata(future.result().text))

    print('Committing scraped race pages to database...')
    Races.add(rows)
