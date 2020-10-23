import pandas as pd

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
        return f"Model: {self.racer_id, self.place}"


class Races2(db.Model):
    race_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String)
    date = db.Column(db.DateTime)
    loc = db.Column(db.String)
    json_url = db.Column(db.String)
    lat = db.Column(db.Float)
    lng = db.Column(db.Float)

    def __repr__(self):
        return f"Model: {self.racer_id, self.place}"


def add_sample_rows():
    """Add some sample rows to the database"""
    for lat, lng in zip([53, 15, 62], [1, 2, 3]):
        row = Races(lat=lat, lng=lng)
        db.session.add(row)
        db.session.commit()

        row = Races2(lat=lat, lng=lng)
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
    df[cols].to_sql('races_dup', db.engine, if_exists='replace')
