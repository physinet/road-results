import model
from database import db

import trueskill as ts

env = ts.TrueSkill(backend='scipy', draw_probability=0)


def _get_ratings(df):
    """Get ratings for a DataFrame of racers. Every place must be unique"""
    return df

    assert (df['Place'].value_counts() == 1).all()

    racers = model.Racers.query.filter(Racers.RacerID.in_(df['RacerID'])).all()
    # df['rating'].update(racers)


def get_ratings(df):
    """Get ratings for a race dataframe. Results will be grouped by category."""
    return df
    df = df.groupby(['RaceCategoryName']).apply(_get_ratings)
    return df
