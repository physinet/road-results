import trueskill as ts
import pandas as pd
from sqlalchemy import update

from database import db

env = ts.TrueSkill(backend='mpmath', draw_probability=0)
default_ratings = {'mu': env.mu, 'sigma': env.sigma, 'num_races': 1}

def get_predicted_places(results):
    """Gets the predicted place for each racer in a set of results. Placing
       order determined by decreasing mean rating."""
    from scipy.stats import rankdata

    ranks = rankdata([-x.prior_mu for x in results], method='min')
    for result, rank in zip(results, ranks):
        result.predicted_place = int(rank) # convert from numpy dtype

def run_trueskill(results):
    """Runs TrueSkill on the race results, where prior ratings are stored
       in prior_mu and prior_sigma attributes for each row in the results.
       Returns a generator of tuples (new_mu, new_sigma) for each row. Returns
       [] if the rating is uncontested and the results are not updated."""

    if len(results) <= 1:  # Uncontested race - only increment num races
        return []

    # TrueSkill requires each "team" as a list. Our teams are one person each
    # and consist of one rating. We then need to get the only element from the
    # returned list to access the updated ratings
    new_ratings = ts.rate([[ts.Rating(result.prior_mu, result.prior_sigma)]
                            for result in results])
    for result, new_rating in zip(results, new_ratings):
        result.new_mu = new_rating[0].mu
        result.new_sigma = new_rating[0].sigma
        if not new_rating[0].mu:
            print(result)
