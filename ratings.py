import time

import trueskill as ts

from itertools import groupby
from sqlalchemy import update
from scipy.stats import rankdata

import config
from database import db
from model import Races, Results, Racers

env = ts.TrueSkill(mu=config.MU, sigma=config.SIGMA,
                   backend='mpmath', draw_probability=0)

def get_all_ratings(debug_limit=None):
    """Get all ratings for results in the Results table."""

    time0 = time.time()

    print('Starting to rate!')
    ordered_results = (Results.query
                              .join(Races, Races.race_id == Results.race_id)
                              .order_by(Races.date,
                                        Results.race_id,
                                        Results.RaceCategoryName,
                                        Results.Place))

    groups = groupby(ordered_results.limit(debug_limit),
                     lambda x: (x.race_id, x.RaceCategoryName))
    print(f'Made groupby: {time.time() - time0}') # 8s for full dataset

    # Warning... this will hog memory!
    # Entire dataset took nearly 3 hours
    for (race_id, category), results in groups:  # ~30s delay to fetch and start
        print(f'Rating race {race_id} category {category}')
        results = list(results)

        # Get Racers rows corresponding to the results
        racers = [Racers.query.get(result.RacerID) for result in results]
        # print(f'Got racers rows: {time.time() - time0}')

        # Store prior ratings in Results table in both prior mu/sigma and
        # current mu/sigma columns - current mu/sigma will change for placing
        # racers and not change for DNF racers
        for result, racer in zip(results, racers):
            result.prior_mu = racer.mu
            result.prior_sigma = racer.sigma
            result.mu = racer.mu
            result.sigma = racer.sigma
        # print(f'store prior ratings: {time.time() - time0}')

        # Predicted placing for ALL racers (including DNFs)
        get_predicted_places(results)
        # print(f'Predicted places: {time.time() - time0}')

        # Filter out DNFs - make lists from pairs of results/racers with
        # valid result.Place
        result_racer_tuples = list(filter(
            lambda x: x[0].Place != None, zip(results, racers)
        ))
        if not result_racer_tuples:
            continue  # empty list!
        placing_results, placing_racers = map(list, zip(*result_racer_tuples))
        # print(f'Filter DNFs: {time.time() - time0}')


        # Rate using trueskill
        if len(placing_results) <= 1:  # don't rate uncontested races
            continue
        new_ratings = run_trueskill(placing_results)
        # print(f'Run trueskill: {time.time() - time0}')

        # Update results and racers rows
        for result, racer, rating in zip(placing_results,
                                         placing_racers,
                                         new_ratings):
            result.mu = racer.mu = rating.mu
            result.sigma = racer.sigma = rating.sigma
            result.rated = True

        print(f'Elapsed time: {time.time() - time0}')

    # Committing took ~15 seconds when stopping entire dataset early, but
    # was instant when done after rating the whole dataset
    time0 = time.time()
    db.session.flush()
    db.session.commit()
    print(f'Committing took: {time.time() - time0}')




def get_predicted_places(results):
    """Gets the predicted place for each racer in a set of results. Placing
       order determined by decreasing mean rating."""

    ranks = rankdata([-x.prior_mu for x in results], method='min')
    for result, rank in zip(results, ranks):
        result.predicted_place = int(rank) # convert from numpy dtype


def reset_ratings():
    """Reset all ratings to default values."""
    defaults = {'mu': env.mu, 'sigma': env.sigma, 'rated': False,
                'prior_mu': env.mu, 'prior_sigma': env.sigma}
    print('Resetting ratings in Results...')
    Results.query.update(defaults, synchronize_session=False)

    defaults = {'mu': env.mu, 'sigma': env.sigma}
    print('Resetting ratings in Racers...')
    Racers.query.update(defaults, synchronize_session=False)
    db.session.flush()
    db.session.commit()


def run_trueskill(results):
    """Runs TrueSkill on the race results, where prior ratings are stored
    in prior_mu and prior_sigma attributes for each row in the results.
    Returns a list of updated results dictionary mappings for each row.
    Returns [] if the rating is uncontested and the results are not updated.
    """

    # TrueSkill requires each "team" as a list. Our teams are one person each
    # and consist of one rating. We then need to get the only element from the
    # returned list to access the updated ratings
    try:
        new_ratings = env.rate([
            [env.Rating(result.prior_mu, result.prior_sigma)]
            for result in results])
        new_ratings = [rating[0] for rating in new_ratings]
    except FloatingPointError as e:
        import dill
        dill.dump([(result.prior_mu, result.prior_sigma) for result in results],
                    open('error.pkl', 'wb'))
        print(e)

    return new_ratings
