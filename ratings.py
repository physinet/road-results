import trueskill as ts
import pandas as pd

from database import db

env = ts.TrueSkill(backend='scipy', draw_probability=0)
default_ratings = {'mu': env.mu, 'sigma': env.sigma, 'num_races': 1}

def default():
    """Returns a default rating as a tuple"""
    return (env.mu, env.sigma, 1)

def run_trueskill(rating_list):
    """Runs TrueSkill on a list of (racer_id, mu, sigma, num_races) tuples
    and returns list of tuples with updated mu, sigma, and num_races + 1"""
    if len(rating_list) <= 1:  # Uncontested race - only increment num races
        return [(r[:-1], r[-1] + 1) for r in rating_list]

    # TrueSkill requires each "team" as a list. Our teams are one person each
    # and consist of one rating. We then need to get the only element from the
    # returned list to access the updated ratings
    new_ratings = ts.rate([[ts.Rating(mu, sigma)]
                           for _, mu, sigma, _ in rating_list])
    return [(r[0], nr[0].mu, nr[0].sigma, r[3] + 1)
            for r, nr in zip(rating_list, new_ratings)]
