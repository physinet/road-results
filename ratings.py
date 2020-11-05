import trueskill as ts

from database import db
from model import Racers, Results

env = ts.TrueSkill(backend='scipy', draw_probability=0)
default_ratings = {'rating_mu': env.mu, 'rating_sigma': env.sigma}

def _get_ratings(df):
    """Get ratings for a DataFrame of racers ordered by placing"""

    # Get existing ratings from racers table
    # This df has columns RacerID, rating_mu, rating_sigma
    existing_ratings = Racers.get_ratings(df['RacerID'])

    # merge into the results dataframe on RacerID
    # fill missing values with default ratings
    df = df.merge(existing_ratings, on=['RacerID'], how='left') \
           .fillna(default_ratings) \
           .rename(columns={'rating_mu': 'prior_rating_mu',
                            'rating_sigma': 'prior_rating_sigma'})

    # Calculate new ratings
    df = df.assign(rating_mu=df['prior_rating_mu'] + 1,
                   rating_sigma=df['prior_rating_sigma'] + 1)

    # Update racers table with new ratings
    df_for_update = df[df['RacerID'].isin(existing_ratings['RacerID'])]
    if not df_for_update.empty:
        Racers.update_ratings(df_for_update)

    return df


def get_ratings(df):
    """Get ratings for a race dataframe. Results will be grouped by category."""
    if df['RaceCategoryName'].nunique() == 1:  # If only one group, apply to whole df
        return _get_ratings(df)
    else:  # There is a confusing issue here if there is only one group
        df = df.groupby('RaceCategoryName').apply(_get_ratings)
        return df
