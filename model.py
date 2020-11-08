import dill
import glob
import os
import pandas as pd
from collections import defaultdict

from sqlalchemy import update
from sqlalchemy import func
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
    index = synonym('RacerID')
    Name = db.Column(db.String)
    Age = db.Column(db.String)
    Category = db.Column(db.Integer)
    mu = db.Column(db.Float, default=ratings.default_ratings['mu'])
    sigma = db.Column(db.Float, default=ratings.default_ratings['sigma'])
    num_races = db.Column(db.Integer, default=1)

    def __repr__(self):
        return f"Racer: {self.RacerID, self.Name, self.mu, self.sigma}"

    @classmethod
    def _add_from_df(cls, df):
        """Add new racers to the racers table from a DataFrame"""
        existing_racers = cls.query \
                             .with_entities(cls.RacerID) \
                             .filter(cls.RacerID.in_(df['RacerID'])) \
                             .all()
        cols = ['RacerID', 'Name', 'Age', 'Category', 'mu', 'sigma']
        rows = ~df['RacerID'].isin(map(lambda x: x[0], existing_racers))

        # update Racers table with new racers, who have now raced 1 time
        df[rows][cols].assign(num_races = 1) \
                      .to_sql('racers', db.engine, if_exists='append',
                        index=False, method='multi')

    @classmethod
    def add_from_df(cls, df):
        """Add to the racers table from a results DataFrame. Add by group to
           avoid duplicate primary keys"""
        if df['RaceCategoryName'].nunique() == 1:  # If only one group, apply to whole df
            return cls._add_from_df(df)
        else:  # There is a confusing issue here if there is only one group
            return df.groupby(['RaceCategoryName']).apply(cls._add_from_df)


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
                           mu=ratings.default_ratings['mu'],
                           sigma=ratings.default_ratings['sigma'],
                           num_races=1)
            db.session.merge(racer)

    @classmethod
    def add_sample(cls):
        for i in [161489, 118930, 149080, 108331]:
            row = cls(RacerID=i, Name='Test', mu=3, sigma=4, num_races=5)
            db.session.add(row)
            db.session.commit()


    @classmethod
    def get_ratings(cls, racer_ids):
        """Get a list of (RacerID, mu, sigma, num_races) tuples given a list of
        RacerID values"""
        existing_ratings = (cls.query
                               .with_entities(cls.RacerID, cls.mu, cls.sigma, cls.num_races)
                               .filter(cls.RacerID.in_(racer_ids))
                               .all())
        return defaultdict(lambda: ratings.default(),
                           {r[0]: r[1:] for r in existing_ratings})


    @classmethod
    def update_ratings(cls, df, existing_ratings):
        """Update ratings given df with RacerID, mu, sigma, and num_races"""
        cols = ['RacerID', 'mu', 'sigma', 'num_races']
        df_for_update = df[df['RacerID'].isin(existing_ratings['RacerID'])]

        if not df_for_update.empty:
            # updated_results is a list of dicts for each row
            updated_results = df_for_update[cols].to_dict('records')
            db.session.bulk_update_mappings(cls, updated_results)
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
    mu = db.Column(db.Float)
    sigma = db.Column(db.Float)

    def __repr__(self):
        return f"Result: {self.index, self.race_id, self.RaceCategoryName, self.Name, self.Place}"

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

def add_table_results():
    files = glob.iglob(os.path.join(
        'C:\\', 'data', 'results', 'races', '*.pkd'))

    print('Building database!')
    for f in files:
        index = int(os.path.split(f)[-1].split('.')[0])  # extract index
        if index < 10000 or index > 10010:
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

def get_all_ratings():
    """Get all ratings from using results in the Results table"""
    results_to_rate = Results.query.filter(Results.Place != None)  # drop DNFs
    places = results_to_rate.with_entities(Results.race_id,
                                           Results.RaceCategoryName,
                                           func.array_agg(Results.RacerID)
                                               .label('racer_id')) \
                            .group_by(Results.race_id,
                                      Results.RaceCategoryName) \
                            .order_by(Results.race_id,
                                      Results.RaceCategoryName)
                                      # TODO: order by date



    for race_id, category, racer_id_list in places:
        # Get a table with the relevant results we want to rate
        results = Results.query.filter(Results.race_id == race_id) \
                               .filter(Results.RaceCategoryName == category) \
                               .filter(Results.RacerID.in_(racer_id_list)) \

        # Update Racers table with new racers (and give them default ratings)
        Racers.add_new_racers(results)

        #


        #
        # joined = results.join(Racers,
        #                       Results.RacerID == Racers.RacerID,
        #                       isouter=True) \
        #                 .with_entities(Results.RacerID, Racers.mu)
        # print(joined.all())




        #
        # print(race_id, racer_id_list)
        # # Get ratings for racers who have already raced
        # existing_ratings = Racers.get_ratings(racer_id_list)
        # # existing_ratings is a defaultdict keyed by racer id
        # # if racer id not in the default dict, will provide initial rating
        # rating_list = [(racer_id, *existing_ratings[racer_id])
        #                 for racer_id in racer_id_list]
        # print(existing_ratings, rating_list)
        #
        # # Update ratings using TrueSkill
        # new_rating_list = ratings.run_trueskill(rating_list)
        # print(new_rating_list)

        # Write updated ratings to Racer table (adding new racers if necessary)


    #
    # updated_results = [Results(index=3, mu=10, sigma=20),
    #                    Results(index=4, mu=20, sigma=30)]
    # db.session.bulk_update_mappings(Results,
    #                                 map(lambda x: x.__dict__, updated_results))
    # db.session.flush()
    # db.session.commit()


        # df = get_ratings(df, existing_ratings)
        # model.Racers.update_ratings(df, existing_ratings)
        # model.Racers.add_from_df(df)  # add only new racers
