import dill
import glob
import os
import pandas as pd

from sqlalchemy import update, func
from sqlalchemy.orm import synonym

from database import db

from preprocess import clean
import ratings


class Races(db.Model):
    race_id = db.Column(db.Integer, primary_key=True)
    index = synonym('race_id')
    name = db.Column(db.String)
    date = db.Column(db.DateTime)
    loc = db.Column(db.String)
    json_url = db.Column(db.String)
    lat = db.Column(db.Float)
    lng = db.Column(db.Float)
    categories = db.Column(db.ARRAY(db.String), default=list)
    _race_names = None

    def __repr__(self):
        return f"Race: {self.race_id, self.name}"

    @classmethod
    def add_from_df(cls, df):
        """Add the race metadata table from a DataFrame"""

        # Change coordinates tuple to two columns
        def get_lat_lng(x):
            if x['coord']:
                x['lat'] = float(x['coord'][0])
                x['lng'] = float(x['coord'][1])
            return x
        df = df.apply(get_lat_lng, axis=1).reset_index()

        cols = ['race_id', 'name', 'date', 'loc', 'json_url', 'lat', 'lng']
        df[cols].to_sql('races', db.engine, if_exists='append',
                index=False, method='multi')

    @classmethod
    def add_categories(cls, race_id, categories):
        """Add a list of categories to the table at the given race_id"""
        # there should only be one row
        row = cls.query.filter(cls.race_id == race_id).one()
        row.categories = categories
        db.session.commit()


    @classmethod
    def get_categories(cls, race_id):
        """Return a list of RaceCategoryNames for the given race_id"""
        return cls.query \
                  .filter(cls.race_id == race_id) \
                  .with_entities(cls.categories) \
                  .one()[0]  # one row, one entity

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
    def get_race_id(cls, name_date):
        """Get the race id for the race with the given name_date
           (i.e., a key in the Races._race_names dictionary)"""
        return cls._get_race_names()[name_date]

    @classmethod
    def get_race_names(cls):
        """Return a list of the names of all races"""
        return list(cls._get_race_names().keys())


class Racers(db.Model):
    RacerID = db.Column(db.Integer, primary_key=True)
    index = synonym('RacerID')
    Name = db.Column(db.String)
    Age = db.Column(db.String)
    Category = db.Column(db.Integer)
    mu = db.Column(db.Float)
    sigma = db.Column(db.Float)
    num_races = db.Column(db.Integer)

    def __repr__(self):
        return f"Racer: {self.RacerID, self.Name, self.mu, self.sigma}"


    @classmethod
    def add_new_racers(cls, results):
        """Given a query consisting of rows from the Results table, adds
           racers to the Racers table that do not already exist in Racers"""
        new_racers = results.join(Racers,
                                  Results.RacerID == Racers.RacerID,
                                  isouter=True) \
                            .filter(Racers.RacerID == None) \
                            .all()
        for new_racer in new_racers:
            racer = Racers(RacerID=new_racer.RacerID,
                           Name=new_racer.Name,
                           Age=new_racer.Age,
                           Category=new_racer.Category,
                           mu=ratings.env.mu,
                           sigma=ratings.env.sigma,
                           num_races=1)
            db.session.merge(racer)


    @classmethod
    def update_ratings(cls, results):
        """Update Racers table from list of rows from Results table."""
        mappings = [{'RacerID': r.RacerID, 'mu': r.new_mu, 'sigma': r.new_sigma}
                    for r in results]
        db.session.bulk_update_mappings(cls, mappings)
        db.session.flush()
        db.session.commit()


class Results(db.Model):
    index = db.Column(db.Integer, primary_key=True)
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
    prior_mu = db.Column(db.Float)
    prior_sigma = db.Column(db.Float)
    new_mu = db.Column(db.Float)
    new_sigma = db.Column(db.Float)
    predicted_place = db.Column(db.Integer)

    def __repr__(self):
        return f"Result: {self.index, self.race_id, self.RaceCategoryName, self.Name, self.Place}"

    @classmethod
    def add_from_df(cls, df):
        """Add to the results table from a DataFrame"""
        cols = ['Place', 'Name', 'Age', 'Category', 'RacerID', 'TeamID',
                'TeamName', 'RaceName', 'RaceCategoryName', 'race_id']
        df[cols].to_sql('results', db.engine, if_exists='append',
                index=False, method='multi')

    @classmethod
    def get_race_table(cls, race_id, RaceCategoryName):
        """For given race_id and RaceCategoryName, returns a generator of
           Results rows"""
        return cls.query \
                  .filter(cls.race_id == race_id,
                          cls.RaceCategoryName == RaceCategoryName,
                          # cls.Place != None
                          ) \
                  .order_by(cls.index)


def add_table_results():
    files = glob.iglob(os.path.join(
        'C:\\', 'data', 'results', 'races', '*.pkd'))

    print('Building database!')
    for f in files:
        index = int(os.path.split(f)[-1].split('.')[0])  # extract index
        if index < 10000 or index > 10011:
            continue
        print(index)
        json = dill.load(open(f, 'rb'))

        df = pd.read_json(json)
        if df.empty:
            continue
        df = clean(df).assign(race_id=index)
        if df.empty:
            continue

        # Add results directly from file without updating ratings
        Results.add_from_df(df)

        # Add categories to the race metadata table
        categories = df['RaceCategoryName'].unique()
        Races.add_categories(index, categories.tolist())

def get_all_ratings():
    """Get all ratings from using results in the Results table"""
    results_to_rate = Results.query.filter(Results.Place != None)  # drop DNFs
    places = results_to_rate.join(Races, Races.race_id == Results.race_id) \
                            .with_entities(Results.race_id,
                                           Results.RaceCategoryName,
                                           Races.date,
                                           func.array_agg(Results.RacerID)
                                               .label('racer_id')) \
                            .group_by(Results.race_id,
                                      Results.RaceCategoryName,
                                      Races.date) \
                            .order_by(Races.date) \



    for race_id, category, date, racer_id_list in places:
        print(f'Rating race {race_id} category {category} date {date}')
        # Get a table with the relevant results we want to rate
        results = Results.query.filter(Results.race_id == race_id) \
                               .filter(Results.RaceCategoryName == category) \
                               .filter(Results.RacerID.in_(racer_id_list))

        # Update Racers table with new racers (and give them default ratings)
        Racers.add_new_racers(results)

        # Join the Results and Racers tables to get prior ratings
        # cte() essentially makes results a subquery like "WITH ... AS ..."
        results_cte = results.cte()  # WITH ... AS ...
        results_racers = \
            db.session \
              .query(results_cte, Racers.mu, Racers.sigma) \
              .join(Racers, results_cte.c.RacerID == Racers.RacerID) \
              .all()

        results = results.all()
        assert len(results_racers) == len(results), \
               'Some racers missing from either Results or Racers table!'
        if len(results) == 1:  # don't rate uncontested races
            continue

        # Store prior ratings in Results table
        for result, result_racers in zip(results, results_racers):
            result.prior_mu = result_racers.mu
            result.prior_sigma = result_racers.sigma

        # Feed the ratings into TrueSkill
        ratings.run_trueskill(results)
        ratings.get_predicted_places(results)

        # Update the racers table with the new ratings
        Racers.update_ratings(results)
