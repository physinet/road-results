import numpy as np

from sqlalchemy import func

from scipy.stats import spearmanr

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

def correlation():
    def test(place, mu):
        print(place, mu)
        return place, mu

    results = Results.query \
                     .filter(Results.Place != None) \
                     .with_entities(Results.RaceName,
                                    Results.RaceCategoryName,
                                    func.array_agg(Results.Place),
                                    func.array_agg(Results.prior_mu)) \
                     .group_by(Results.RaceName, Results.RaceCategoryName)

    num_racers = []
    correlations = []
    print('Calculating Spearman correlation!')
    for name, category, places, mus in results:
        correlations.append(spearmanr(places, mus).correlation)
        num_racers.append(len(places))
        if num_racers[-1] > 10 and correlations[-1] > 0.9:
            print(name, category)
    print('Done calculating Spearman correlation!')

    np.save('num_racers.npy', np.array(num_racers))
    np.save('correlations.npy', np.array(correlations))


def get_rating_hist():
    """Get a histogram of all mean ratings."""
    racers = Racers.query.filter(Racers.mu!=25).all()
    mus = [racer.mu for racer in racers]
    hist, bin_edges = np.histogram([racer.mu for racer in racers], bins=50)
    np.save('mus.npy', np.array(mus))
    return hist, bin_edges
