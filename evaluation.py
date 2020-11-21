import numpy as np

from model import Racers, Races, Results

def accuracy():
    # We only want to compute accuracy for results that have already been rated
    # We also need to exclude DNF racers - there is not much to learn from them
    # Should we also filter to races with at least N racers?
    # e.g. a race with 5 people is much more likely to have high accuracy
    # than a race with 50
    rated = Results.query.filter(Results.rated, Results.Place != None).yield_per(1000)
    for result in rated:
        print(result.Place, result.predicted_place)

def get_rating_hist():
    """Get a histogram of all mean ratings."""
    racers = Racers.query.filter(Racers.mu!=25).all()
    hist, bin_edges = np.histogram([racer.mu for racer in racers], bins=50)
    return hist, bin_edges
