import trueskill as ts

from sqlalchemy import update
from scipy.stats import rankdata

from database import db


env = ts.TrueSkill(backend='mpmath', draw_probability=0)

def get_predicted_places(results):
    """Gets the predicted place for each racer in a set of results. Placing
       order determined by decreasing mean rating."""

    ranks = rankdata([-x.prior_mu for x in results], method='min')
    for result, rank in zip(results, ranks):
        result.predicted_place = int(rank) # convert from numpy dtype

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
        new_ratings = env.rate([[env.Rating(result.prior_mu, result.prior_sigma)]
                                for result in results])
        new_ratings = [rating[0] for rating in new_ratings]
    except FloatingPointError as e:
        import dill
        dill.dump([(result.prior_mu, result.prior_sigma) for result in results],
                    open('error.pkl', 'wb'))
        print(e)

    return new_ratings
