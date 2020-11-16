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
    num_racers = db.Column(db.ARRAY(db.Integer), default=list) # number of races per category
    _race_names = None

    def __repr__(self):
        return f"Race: {self.race_id, self.name}"

    @classmethod
    def add(cls, rows):
        """Add rows to the Races table. rows is a list of dictionaries
        with keys matching the column names for the table.
        """
        db.session.add_all([cls(**row) for row in rows])
        db.session.commit()

    @classmethod
    def add_at_id(cls, race_id, attribs):
        """Add attributes to the table at the given race_id. For example,
           if we want to add a list of categories, then attribs is a
           dictionary like: {'categories': ['Cat 1', 'Cat 2', 'Cat 3']}"""
        # there should only be one row
        row = cls.query.filter(cls.race_id == race_id).one()
        for attrib, val in attribs.items():
            setattr(row, attrib, val)
        db.session.commit()


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


class Racers(db.Model):
    RacerID = db.Column(db.Integer, primary_key=True)
    index = synonym('RacerID')
    Name = db.Column(db.String)
    Age = db.Column(db.String)
    Category = db.Column(db.Integer)
    mu = db.Column(db.Float)
    sigma = db.Column(db.Float)
    num_races = db.Column(db.Integer)
    _racer_names = None

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

    @classmethod
    def get_racer_results(cls, racer_id):
        """For a given RacerID, returns a generator of Results rows for that
           racer."""
        return cls.query \
                  .filter(cls.RacerID == racer_id) \
                  .order_by(cls.index)


def add_table_results(id_range=(0,13000)):
    """Add to the Results table from locally saved pickled json files.
    id_range sets the range of race_ids we should load into the database.
    """
    files = glob.iglob(os.path.join(
        'C:\\', 'data', 'results', 'races', '*.pkd'))

    id_min, id_max = id_range

    print('Building database!')
    for f in files:
        index = int(os.path.split(f)[-1].split('.')[0])  # extract index
        if index < id_min or index > id_max:
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
        num_races = [len(df[df['RaceCategoryName'] == cat]) for cat in categories]

        Races.add_at_id(index, {'categories': categories.tolist(),
                                'num_racers': num_races})

def filter_races():
    """Drop rows from the races table that correspond to races NOT represented
       in the Results table - these are not in the database yet"""
    races = Results.query.with_entities(Results.race_id).distinct().all()
    races = [race for race, in races]
    Races.query.filter(~Races.race_id.in_(races)).delete('fetch')
    db.session.commit()

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
