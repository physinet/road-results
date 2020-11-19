import dill
import glob
import os
import re
import time
import pandas as pd

from itertools import groupby

import sqlalchemy as sa
from sqlalchemy import update, func
from sqlalchemy.orm import synonym

from database import db

from preprocess import clean
import ratings
import scraping



class Model:
    def __init__(self, **entries):
        """Custom init to ignore keyword arguments not in the table schema."""
        d = {col: entries.get(col) for col in self.get_columns()}
        self.__dict__.update(d)


    @classmethod
    def add(cls, rows):
        """Add rows to the table. rows is a list of dictionaries
        with keys matching the column names for the table.
        """
        objs = [cls(**row) if isinstance(row, dict)
                   else cls(**row.__dict__)
                   for row in filter(lambda x: x, rows)] # filter empty rows
        objs = cls.drop_duplicates(objs)

        db.session.add_all(objs)
        db.session.commit()

    @classmethod
    def count(cls):
        """Returns a count of the rows in the table."""
        return cls.query.count()

    @classmethod
    def drop_duplicates(cls, rows):
        """Check list of row objects duplicate index and return list of row
        objects with unique primary keys.
        """
        keys = set()
        final_rows = []
        for i, row in enumerate(rows):
            if row.index not in keys:
                keys.add(row.index)
                final_rows.append(row)
        return final_rows

    @classmethod
    def get_column(cls, col):
        """Get a list of all distinct values in specified table column."""
        return sorted(list(x for x, in cls.query
                                          .with_entities(getattr(cls, col))
                                          .distinct()
                                          .all()))

    @classmethod
    def get_columns(cls):
        """Get the column names for the table."""
        return list(cls.__table__.columns.keys())

    @classmethod
    def get_sample(cls, limit, start=0):
        """Get the first `limit` rows of the table starting at index start.
        """
        return (cls.query
                   .filter(cls.index >= start)
                   .order_by(cls.index)
                   .limit(limit))

    @classmethod
    def update(cls, rows, commit=True):
        """Update table from list of rows, which may be from other tables.
        Only attributes of each row matching column names in the table
        will be considered.
        """
        if not rows: # empty list
            return

        # Filter empty rows and take dictionary if each row is an object
        if not isinstance(rows[0], dict):
            rows = map(lambda x: x.__dict__, filter(lambda x: x, rows))

        cols = {col for col in cls.get_columns()}

        # only map attributes that are named columns in this table
        mappings = [{col: row.get(col) for col in (cols & row.keys())}
                     for row in rows]

        db.session.bulk_update_mappings(cls, mappings)
        if commit:
            db.session.flush()
            db.session.commit()


class Races(Model, db.Model):
    race_id = db.Column(db.Integer, primary_key=True)
    index = synonym('race_id')
    name = db.Column(db.String)
    date = db.Column(db.DateTime)
    loc = db.Column(db.String)
    json_url = db.Column(db.String)
    lat = db.Column(db.Float)
    lng = db.Column(db.Float)
    categories = db.Column(db.ARRAY(db.String), default=list)
    num_racers = db.Column(db.ARRAY(db.Integer), default=list) # number of races per category
    _race_names = None

    def __repr__(self):
        return f"Race: {self.race_id, self.name}"

    @classmethod
    def add_table(cls, race_ids=list(range(1, 13000))):
        time0 = time.time()

        # Only add race_ids not yet in table
        race_ids = sorted(list(set(race_ids) - cls.get_column('race_id')))

        print('Scraping BikeReg race pages for metadata...')
        futures = scraping.get_futures(race_ids)
        for race_id, future in zip(race_ids, futures):
            row = scraping.scrape_race_page(race_id, future.result().text)
            cls.add([row])
            print('Elapsed time: ', time.time() - time0)

    @classmethod
    def get_categories(cls, race_id):
        """Return a list of RaceCategoryNames for the given race_id"""
        return cls.query \
                  .filter(cls.race_id == race_id) \
                  .with_entities(cls.categories) \
                  .one()[0]  # one row, one entity

    @classmethod
    def get_race_id(cls, name_date):
        """Get the race id for the race with the given name_date
           (i.e., a key in the Races._race_names dictionary).
           Returns None for invalid name_date."""
        return cls._get_race_names().get(name_date)

    @classmethod
    def _get_race_names(cls):
        """Store a dictionary with keys corresponding to a name - date
           string and values equal to the correpsonding race ids."""
        if not cls._race_names:
            cls._race_names = {'{} ({})'.format(race.name,
                                   race.date.strftime('%Y-%m-%d')): race.race_id
                                  for race in cls.query.distinct().all()}
        return cls._race_names

    @classmethod
    def get_race_name_date(cls, race_id):
        """Get the name and date of the race with given race_id"""
        name, date =  cls.query.filter(cls.race_id == race_id) \
                         .with_entities(cls.name, cls.date).one()
        return '{} ({})'.format(name, date.strftime('%Y-%m-%d'))

    @classmethod
    def get_race_names(cls):
        """Return a list of the names of all races"""
        return list(cls._get_race_names().keys())

    @classmethod
    def get_urls(cls):
        """Return a list of JSON results file URLs"""
        return list(map(lambda x: x[0], cls.query
                                           .with_entities(cls.json_url)
                                           .order_by(cls.race_id)
                                           .all()))

class Racers(Model, db.Model):
    RacerID = db.Column(db.Integer, primary_key=True)
    index = synonym('RacerID')
    Name = db.Column(db.String)
    Age = db.Column(db.String)
    Category = db.Column(db.Integer)
    mu = db.Column(db.Float, default=ratings.env.mu)
    sigma = db.Column(db.Float, default=ratings.env.sigma)
    num_races = db.Column(db.Integer, default=1)
    _racer_names = None

    def __repr__(self):
        return f"Racer: {self.RacerID, self.Name, self.mu, self.sigma}"

    @classmethod
    def add_table(cls):
        """Add racers to the table from the Results table."""
        # For each RacerID, take "max" (only) name, max age, and min category.
        rows = (Results.query
                       .with_entities(Results.RacerID,
                                      func.max(Results.Name),
                                      func.max(Results.Age),
                                      func.min(Results.Category))
                       .group_by(Results.RacerID))
        rows = ({'RacerID': row[0], 'Name': row[1],
                 'Age': row[2], 'Category': row[3]} for row in rows)
        cls.add(rows)

    @classmethod
    def get_racer_id(cls, name):
        """Get the race id for the given racer name
           (i.e., a key in the Racers._racer_names dictionary).
           Returns None for invalid racer name."""
        return cls._get_racer_names().get(name)

    @classmethod
    def get_racer_name(cls, RacerID):
        """Get the name of the racer with given RacerID"""
        return cls.query.filter(cls.RacerID == RacerID) \
                  .with_entities(cls.Name).one()[0]


    @classmethod
    def _get_racer_names(cls):
        """Store a dictionary with keys corresponding to racer names
            and values equal to the correpsonding race ids."""
        if not cls._racer_names:
            cls._racer_names = {racer.Name: racer.RacerID
                                  for racer in cls.query.distinct().all()}
        return cls._racer_names


    @classmethod
    def get_racer_names(cls):
        """Returns a list of the names of all racers"""
        return list(cls._get_racer_names().keys())


class Results(Model, db.Model):
    ResultID = db.Column(db.Integer, primary_key=True)
    index = synonym('ResultID')
    Place = db.Column(db.Integer)
    Name = db.Column(db.String)
    Age = db.Column(db.Integer)
    Category = db.Column(db.Integer)
    RacerID = db.Column(db.Integer)
    TeamID = db.Column(db.Integer)
    TeamName = db.Column(db.String)
    RaceName = db.Column(db.String)
    RaceCategoryName = db.Column(db.String)
    race_id = db.Column(db.Integer)
    prior_mu = db.Column(db.Float, default=ratings.env.mu)
    prior_sigma = db.Column(db.Float, default=ratings.env.sigma)
    mu = db.Column(db.Float, default=ratings.env.mu)
    sigma = db.Column(db.Float,  default=ratings.env.sigma)
    predicted_place = db.Column(db.Integer)

    def __repr__(self):
        return f"Result: {self.index, self.race_id, self.RaceCategoryName, self.Name, self.Place}"


    @classmethod
    def add_table(cls, urls):
        """Add results from a list of BikeReg JSON file URLs. These can be
        obtained from the Races table using Races.get_urls().
        """

        # Only add results with race_ids not yet in table
        d = {int(re.search('(\d+)', url).group()): url for url in urls}
        for race_id in cls.get_column('race_id'):
            d.pop(race_id, None) # remove race_ids already in table

        race_ids, urls = zip(*sorted(zip(d.keys(), d.values())))

        time0 = time.time()
        print('Scraping BikeReg JSON results files...')
        futures = scraping.get_results_futures(urls)
        for race_id, url, future in zip(race_ids, urls, futures):
            print(race_id)
            rows = scraping.scrape_results_json(race_id, future.result().text)
            cls.add(rows)
            print('Elapsed time: ', time.time() - time0)

        print('Populating categories...')
        add_categories()
        print('Elapsed time: ', time.time() - time0)

    @classmethod
    def get_race_table(cls, race_id, RaceCategoryName):
        """For given race_id and RaceCategoryName, returns a generator of
           Results rows"""
        return cls.query \
                  .filter(cls.race_id == race_id,
                          cls.RaceCategoryName == RaceCategoryName,
                          ) \
                  .order_by(cls.index)

    @classmethod
    def get_racer_results(cls, racer_id):
        """For a given RacerID, returns Results rows for that racer."""
        return cls.query.filter(cls.RacerID == racer_id)

def add_categories():
    """Update the Races table with the categories represented in the
    Results table.
    """
    # results of query are (arbitrary ResultID (so we can order), Race ID,
    # category name, number of racers in that category)
    cat_counts = (Results.query
                         .with_entities(
                            Results.race_id,
                            Results.RaceCategoryName,
                            func.count(Results.RaceCategoryName).label('count'))
                         .group_by(Results.race_id,
                                   Results.RaceCategoryName)
                         .order_by(Results.RaceCategoryName)
                         .subquery('cat_counts'))
    collected = (db.session
                   .query(cat_counts.c.race_id,
                          func.array_agg(cat_counts.c.RaceCategoryName).label('categories'),
                          func.array_agg(cat_counts.c.count).label('num_racers'))
                   .group_by(cat_counts.c.race_id))
    Races.update([c._asdict() for c in collected])


def filter_races():
    """Drop rows from the races table that correspond to races NOT represented
    in the Results table - these are not in the database yet.
    """
    races = Results.query.with_entities(Results.race_id).distinct().all()
    races = [race for race, in races]
    Races.query.filter(~Races.race_id.in_(races)).delete('fetch')
    db.session.commit()

def get_all_ratings():
    """Get all ratings for results in the Results table."""

    import time
    time0 = time.time()

    print('Starting to rate!')
    ordered_results = (Results.query
                              .join(Races, Races.race_id == Results.race_id)
                              .order_by(Races.date,
                                        Results.race_id,
                                        Results.RaceCategoryName,
                                        Results.Place))

    groups = groupby(ordered_results, lambda x: (x.race_id, x.RaceCategoryName))
    print(f'Made groupby: {time.time() - time0}') # 8s for full dataset

    # Warning... this will hog memory!
    for (race_id, category), results in groups:  # ~30s delay to fetch and start
        print(f'Rating race {race_id} category {category}')
        results = list(results)

        # Get Racers rows corresponding to the results
        racers = [Racers.query.get(result.RacerID) for result in results]
        # print(f'Got racers rows: {time.time() - time0}')

        # Store prior ratings in Results table in both prior mu/sigma and
        # current mu/sigma columns - current mu/sigma will change for placing
        # racers and not change for DNF racers
        for result, racer in zip(results, racers):
            result.prior_mu = racer.mu
            result.prior_sigma = racer.sigma
            result.mu = racer.mu
            result.sigma = racer.sigma
        # print(f'store prior ratings: {time.time() - time0}')

        # Predicted placing for ALL racers (including DNFs)
        ratings.get_predicted_places(results)
        # print(f'Predicted places: {time.time() - time0}')

        # Filter out DNFs - make lists from pairs of results/racers with
        # valid result.Place
        result_racer_tuples = list(filter(
            lambda x: x[0].Place != None, zip(results, racers)
        ))
        if not result_racer_tuples:
            continue  # empty list!
        placing_results, placing_racers = map(list, zip(*result_racer_tuples))

        # Rate using trueskill
        if len(placing_results) <= 1:  # don't rate uncontested races
            continue
        new_ratings = ratings.run_trueskill(placing_results)

        # Update results and racers rows
        for result, racer, rating in zip(placing_results,
                                         placing_racers,
                                         new_ratings):
            result.mu = rating.mu
            result.sigma = rating.sigma
            racer.mu = rating.mu
            racer.sigma = rating.sigma

        print(f'Elapsed time: {time.time() - time0}')
        if time.time()-time0 > 240:
            break

    # Committing can take ~15 seconds with entire dataset, regardless of how
    # many rows were updated
    time0 = time.time()
    db.session.flush()
    db.session.commit()
    print(f'Committing took: {time.time() - time0}')

def get_racer_table(racer_id):
    """Returns a list of dictionaries of results for the given racer id.
    Each dictionary is essentially a Results row object with some
    information joined from the Races table: date and num_racers,
    where num_racers is the appropriate number of racers for the
    RaceCategoryName associated with the result.
    """
    racer_results = Results.get_racer_results(racer_id)
    race_metadata = racer_results.join(Races, Races.race_id == Results.race_id) \
                                 .with_entities(Results,
                                                Races.date,
                                                Races.categories,
                                                Races.num_racers) \
                                 .order_by(Races.date.desc())

    def get_num_racers(category, categories, num_racers):
        return num_racers[categories.index(category)]

    dict_merge = lambda a,b: a.update(b) or a  # updates a with b then returns a

    racer_table = []
    for row, date, categories, num_racers in race_metadata:
        meta = {'date': date,
                'num_racers': get_num_racers(row.RaceCategoryName,
                                             categories, num_racers)}
        racer_table.append(dict_merge(row.__dict__, meta))
    return racer_table

def reset_ratings():
    """Reset all ratings to default values."""
    defaults = {'mu': ratings.env.mu, 'sigma': ratings.env.sigma,
                'prior_mu': ratings.env.mu, 'prior_sigma': ratings.env.sigma}
    print('Resetting ratings in Results...')
    Results.query.update(defaults, synchronize_session=False)

    defaults = {'mu': ratings.env.mu, 'sigma': ratings.env.sigma}
    print('Resetting ratings in Racers...')
    Racers.query.update(defaults, synchronize_session=False)
    db.session.flush()
    db.session.commit()
