import trueskill as ts

from database import db
from model import Racers, Results

env = ts.TrueSkill(backend='scipy', draw_probability=0)


def _get_ratings(df):
    """Get ratings for a DataFrame of racers ordered by placing"""

    # Insert results into table with default ratings
    df['prior_rating_mu'] = env.mu
    df['prior_rating_sigma'] = env.sigma

    # Join with racers table and update ratings in results table if they exist
    racers = df['RacerID']
    existing_ratings = Racers.get_ratings(racers)

    # print(existing_ratings)

    # Calculate new ratings
    df = df.assign(rating_mu=df['prior_rating_mu'],
                   rating_sigma=df['prior_rating_sigma'])

    # Update racers table with new ratings
    Racers.update_ratings(racers, df['rating_mu'], df['rating_sigma'])
    # df_racers = df[['RacerID', 'Name', 'Age', 'Category', 'rating_mu', 'rating_sigma']]
    # df_racers.to_sql('racers', db.bind, if_exists='append', index=False, method='multi')

    return df

    # racers = model.Racers.query.filter(Racers.RacerID.in_(df['RacerID'])).all()
    # df['rating'].update(racers)


def get_ratings(df):
    """Get ratings for a race dataframe. Results will be grouped by category."""
    if df['RaceCategoryName'].nunique() == 1:  # If only one group, apply to whole df
        df = _get_ratings(df)
    else:  # There is a confusing issue here if there is only one group
        df = df.groupby(['RaceCategoryName']).apply(_get_ratings)
    return df
