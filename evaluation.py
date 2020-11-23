import numpy as np

from sqlalchemy import func

from scipy.stats import spearmanr

from model import Racers, Races, Results

def correlation(min_racers=5):
    """Calculates the Spearman correlation for races with the given minimum
    number of racers."""

    results = Results.query \
                     .filter(Results.Place != None) \
                     .with_entities(Results.RaceName,
                                    Results.RaceCategoryName,
                                    func.array_agg(Results.Place),
                                    func.array_agg(Results.prior_mu)) \
                     .group_by(Results.RaceName, Results.RaceCategoryName)

    print('Calculating Spearman correlation...')
    correlations = []
    for name, category, places, mus in results:
        if len(places) >= min_racers:
            correlations.append(spearmanr(places, mus).correlation)
    print('Done calculating Spearman correlation!')

    return correlations


def get_mean_ratings():
    """Get all mean ratings."""
    racers = Racers.query.filter(Racers.mu != 25).all()
    return [racer.mu for racer in racers]
