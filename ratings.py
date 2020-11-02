import trueskill as ts

from database import db
from model import Racers, Results

env = ts.TrueSkill(backend='scipy', draw_probability=0)


def _get_ratings(df):
    """Get ratings for a DataFrame of racers ordered by placing"""
    # Insert results into table with default ratings
    df['prior_rating_mu'] = env.mu
    df['prior_rating_sigma'] = env.sigma
    df['rating_mu'] = env.mu
    df['rating_sigma'] = env.sigma

    # Get existing ratings from racers table and update prior ratings
    existing_ratings = Racers.get_ratings(df['RacerID'])
    # df = df.merge(existing_ratings, on=['RacerID', ''] how='left')
    df = df.merge(existing_ratings, on=['RacerID'], how='left')

    df['prior_rating_mu'] = df['rating_mu_y'].fillna(df['rating_mu_x'])
    df['prior_rating_sigma'] = df['rating_sigma_y'].fillna(df['rating_sigma_x'])
    df = df.drop(columns=['rating_mu_x', 'rating_mu_y',
                    'rating_sigma_x', 'rating_sigma_y'])

    # Calculate new ratings
    df = df.assign(rating_mu=df['prior_rating_mu'] * 2,
                   rating_sigma=df['prior_rating_sigma'] ** 4)

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
