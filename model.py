import dill
import glob
import os
import re
import time
import pandas as pd

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
    def update(cls, rows):
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
    def add(cls, results):
        """Add rows to the table from results rows. Will only attempt to add
        rows with new RacerIDs not already in the table.
        """
        new = results.filter(Results.RacerID.notin_(
                                Racers.query.with_entities(Racers.RacerID)
                            ))
        super().add(new)

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
    mu = db.Column(db.Float)
    sigma = db.Column(db.Float)
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
        """For a given RacerID, returns a generator of Results rows for that
           racer."""
        return cls.query \
                  .filter(cls.RacerID == racer_id) \
                  .order_by(cls.index)

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

    results_groups = (
        Results.query
               .join(Races, Races.race_id == Results.race_id)
               .with_entities(Results.race_id,
                              Results.RaceCategoryName,
                              Races.date,
                              func.array_agg(Results.RacerID).label('id_list'))
               .group_by(Results.race_id,
                         Results.RaceCategoryName,
                         Races.date)
               .order_by(Races.date)
    )

    for race_id, category, date, racer_id_list in results_groups:
        print(f'Rating race {race_id} category {category} date {date}')
        # Get a table with the relevant results we want to rate
        results = Results.query.filter(Results.race_id == race_id) \
                               .filter(Results.RaceCategoryName == category) \
                               .filter(Results.RacerID.in_(racer_id_list))

        if results.count() == 1:  # don't rate uncontested races
            continue

        # Update Racers table with new racers (and give them default ratings)
        Racers.add(results)

        # Join the Results and Racers tables to get prior ratings
        # cte() essentially makes results a subquery like "WITH ... AS ..."
        results_cte = results.cte()  # WITH ... AS ...
        results_racers = \
            db.session \
              .query(results_cte, Racers.mu, Racers.sigma) \
              .join(Racers, results_cte.c.RacerID == Racers.RacerID)

        assert results_racers.count() == results.count(), \
               'Some racers missing from either Results or Racers table!'


        # Store prior ratings in Results table
        for result, result_racers in zip(results, results_racers):
            result.prior_mu = result_racers.mu
            result.prior_sigma = result_racers.sigma
            # Also record the prior ratings in the "current" ratings columns
            # run_trueskill will update these for every placing racer
            result.mu = result_racers.mu
            result.sigma = result_racers.sigma

        # Predicted placing for ALL racers (including DNFs)
        ratings.get_predicted_places(results)

        # Feed the ratings into TrueSkill - rate only those that placed
        mappings = ratings.run_trueskill(results.filter(Results.Place != None))
        Results.update(mappings)

        # Update the racers table with the new ratings
        Racers.update(results)


def get_racer_table(racer_id):
    """Returns a list of dictionaries of results for the given racer id.
       Each dictionary is essentially a Results row object with some
       information joined from the Races table: date and num_racers,
       where num_racers is the appropriate number of racers for the
       RaceCategoryName associated with the result."""
    racer_results = Results.get_racer_results(racer_id)
    race_metadata = racer_results.join(Races, Races.race_id == Results.race_id) \
                                 .with_entities(Races.date,
                                                Races.categories,
                                                Races.num_racers)

    def get_num_racers(category, categories, num_racers):
        return num_racers[categories.index(category)]

    dict_merge = lambda a,b: a.update(b) or a  # updates a with b then returns a

    racer_table = []
    for row, row_meta in zip(racer_results, race_metadata):
        meta = {'date': row_meta[0],
                'num_racers': get_num_racers(row.RaceCategoryName,
                                             *row_meta[1:])}
        racer_table.append(dict_merge(row.__dict__, meta))

    return racer_table
