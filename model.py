import dill
import glob
import os
import pandas as pd

from database import db

from preprocess import clean
from ratings import get_ratings


class Races(db.Model):
    race_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String)
    date = db.Column(db.DateTime)
    loc = db.Column(db.String)
    json_url = db.Column(db.String)
    lat = db.Column(db.Float)
    lng = db.Column(db.Float)

    def __repr__(self):
        return f"Race: {self.race_id, self.name}"


class Racers(db.Model):
    RacerID = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String)
    Age = db.Column(db.String)
    Category = db.Column(db.Integer)
    rating_mu = db.Column(db.Float)
    rating_sigma = db.Column(db.Float)

    def __repr__(self):
        return f"Racer: {self.RacerID, self.Name, self.rating_mu, self.rating_sigma}"


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
    prior_rating_mu = db.Column(db.Float)
    prior_rating_sigma = db.Column(db.Float)
    rating_mu = db.Column(db.Float)
    rating_sigma = db.Column(db.Float)

    def __repr__(self):
        return f"Result: {self.index, self.RaceName, self.Name}"


def add_sample_rows():
    """Add some sample rows to the database"""
    for lat, lng in zip([53, 15, 62], [1, 2, 3]):
        row = Races(lat=lat, lng=lng)
        db.session.add(row)
        db.session.commit()


def add_table_races():
    """Add the race metadata table from locally saved DataFrame"""
    df = pd.read_pickle('C:/data/results/df.pkl')

    # Change coordinates tuple to two columns
    def get_lat_lng(x):
        if x['coord']:
            x['lat'] = float(x['coord'][0])
            x['lng'] = float(x['coord'][1])
        return x
    df = df.apply(get_lat_lng, axis=1).reset_index()

    cols = ['race_id', 'name', 'date', 'loc', 'json_url', 'lat', 'lng']
    df[cols].to_sql('races', db.engine, if_exists='replace')


def add_table_results():
    """Add the results tables from locally saved DataFrames"""
    files = glob.iglob(os.path.join(
        'C:\\', 'data', 'results', 'races', '*.pkd'))

    print('Building database!')
    for f in files:
        index = int(os.path.split(f)[-1].split('.')[0])  # extract index
        if index < 10000 or index > 10100:
            continue
        print(index)
        json = dill.load(open(f, 'rb'))

        df = pd.read_json(json)
        if df.empty:
            continue
        df = clean(df).assign(race_id=index)

        df = get_ratings(df)

        cols = ['Place', 'Name', 'Age', 'Category', 'RacerID', 'TeamID',
                'TeamName', 'RaceName', 'RaceCategoryName', 'race_id']
        df[cols].to_sql('results', db.engine, if_exists='append', index=False)
