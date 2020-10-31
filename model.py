import dill
import glob
import os
import pandas as pd

from sqlalchemy import update

from database import db



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

    @classmethod
    def add_from_df(cls, df):
        """Add the race metadata table from a DataFrame"""
        raise Exception('Move df loading to external!!')
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


class Racers(db.Model):
    RacerID = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String)
    Age = db.Column(db.String)
    Category = db.Column(db.Integer)
    rating_mu = db.Column(db.Float)
    rating_sigma = db.Column(db.Float)

    def __repr__(self):
        return f"Racer: {self.RacerID, self.Name, self.rating_mu, self.rating_sigma}"

    @classmethod
    def _add_from_df(cls, df):
        """Add to the racers table from a DataFrame"""
        existing_racers = cls.query \
                             .with_entities(cls.RacerID) \
                             .filter(cls.RacerID.in_(df['RacerID'])) \
                             .all()
        cols = ['RacerID', 'Name', 'Age', 'Category', 'rating_mu', 'rating_sigma']
        rows = ~df['RacerID'].isin(map(lambda x: x[0], existing_racers))
        df[rows][cols].to_sql('racers', db.engine, if_exists='append',
                index=False, method='multi')


    @classmethod
    def add_from_df(cls, df):
        """Add to the racers table from a results DataFrame. Add by group to
           avoid primary keys"""

        df = df.groupby(['RaceCategoryName']).apply(cls._add_from_df)
        return df


    @classmethod
    def get_ratings(cls, racer_ids):
        """Get ratings given a list of RacerID values"""
        return cls.query\
                  .with_entities(cls.RacerID,
                                 cls.rating_mu,
                                 cls.rating_sigma) \
                  .filter(cls.RacerID.in_(racer_ids)) \
                  .all()

    @classmethod
    def update_ratings(cls, racers, rating_mu, rating_sigma):
        """Update ratings given lists of RacerID values, mu, and sigma"""
        return
        cls.merge().where(cls.RacerID.in_(racers)).values(
            rating_mu=rating_mu,
            rating_sigma=rating_sigma
        )
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
    prior_rating_mu = db.Column(db.Float)
    prior_rating_sigma = db.Column(db.Float)
    rating_mu = db.Column(db.Float)
    rating_sigma = db.Column(db.Float)

    def __repr__(self):
        return f"Result: {self.index, self.RaceName, self.Name}"

    @classmethod
    def add_from_df(cls, df):
        """Add to the results table from a DataFrame"""
        cols = ['Place', 'Name', 'Age', 'Category', 'RacerID', 'TeamID',
                'TeamName', 'RaceName', 'RaceCategoryName', 'race_id']
        df[cols].to_sql('results', db.engine, if_exists='append',
                index=False, method='multi')



def add_sample_rows():
    """Add some sample rows to the database"""
    for lat, lng in zip([53, 15, 62], [1, 2, 3]):
        row = Races(lat=lat, lng=lng)
        db.session.add(row)
        db.session.commit()
