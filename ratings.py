import trueskill as ts
import pandas as pd

from database import db
from model import Racers, Results

env = ts.TrueSkill(backend='scipy', draw_probability=0)
default_ratings = {'mu': env.mu, 'sigma': env.sigma}

def run_trueskill(df):
    """Runs trueskill on a dataframe with columns prior_mu,
    prior_sigma and updates ratings in columns _mu, sigma"""
    prior_mu, prior_sigma = df['prior_mu'], df['prior_sigma']

    if len(df) <= 1:  # Uncontested race
        return df.assign(mu=prior_mu, sigma=prior_sigma)

    # TrueSkill requires each "team" as a list. Our teams are one person each
    # and consist of one rating. We then need to get the only element from the
    # returned list to access the updated ratings
    new_ratings = ts.rate([[ts.Rating(mu, sigma)]
                           for mu, sigma in zip(prior_mu, prior_sigma)])
    new_mu, new_sigma = map(pd.Series, zip(*[(r[0].mu, r[0].sigma)
                                                for r in new_ratings]))

    return df.assign(mu=new_mu, sigma=new_sigma)

def _get_ratings(df):
    """Get ratings for a DataFrame of racers ordered by placing"""

    # Get existing ratings from racers table
    # This df has columns RacerID, mu, sigma
    existing_ratings = Racers.get_ratings(df['RacerID'])

    # merge into the results dataframe on RacerID
    # fill missing values with default ratings
    df = df.merge(existing_ratings, on=['RacerID'], how='left') \
           .fillna(default_ratings) \
           .rename(columns={'mu': 'prior_mu',
                            'sigma': 'prior_sigma'})

    # Calculate new ratings
    df = run_trueskill(df)

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
