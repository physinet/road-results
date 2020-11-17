# BikeRank: skill ratings for amateur road cyclists

This is a **working draft** of an application created by [Brian Schaefer](https://www.linkedin.com/in/briantschaefer/) for [The Data Incubator](https://www.thedataincubator.com/) (Fall 2020 cohort).
This project will be finalized by **December 1, 2020**. Until then, please excuse any shortcomings.

BikeRank adopts the [TrueSkill™ Ranking System](https://www.microsoft.com/en-us/research/project/trueskill-ranking-system/) to [amateur road cycling races](https://results.bikereg.com/).
View this project online at: http://bike-rank.herokuapp.com/ (may take a few seconds on first load).
Here, you view ratings either by race or by racer.
Viewing by race, you can see how the ratings are updated as a result of each individual race.
Viewing by racer, you can see how each racer's rating changes over time.

# Architecture
This project combines an assortment of techniques new to the author, each explained briefly below.

Details to be added...

## Web scraping
## Database
The relevant data for this project are stored in a PostgreSQL database hosted on [AWS](https://aws.amazon.com/rds/) with three tables:
- `Races`: Each row corresponds to one race event identified by a unique `race_id`. This table stores the relevant metadata for each race, including its name, date, location, list of race categories, and the number of racers competing in each category.
- `Racers`: Each row corresponds to one racer identified by a unique `RacerID`. This table primarily stores the racer's name and current skill rating (parameterized by a mean skill rating `mu` and uncertainty `sigma`). The ratings in this table are updated upon processing each additional race.
- `Results`: Each row corresponds to one result: the finishing place for one racer in one category of one race, along with the corresponding ID numbers for each. This table also records the skill rating of each racer both prior to (`prior_mu`, `prior_sigma`) and as a result of (`mu`, `sigma`) the race outcome.

In `model.py`, I represent the tables using [SQLAlchemy](https://docs.sqlalchemy.org/en/13/orm/tutorial.html) classes. Each class has its own helper functions to query the database,
and there are additional functions defined in `model.py` to apply the rating system
to the database and collect data relevant for the website.

## TrueSkill
I have adapted the Python implementation of [TrueSkill](https://trueskill.org/) to determine skill ratings for the racers represented in the dataset. TrueSkill takes a list of skill ratings ordered by final placing and updates each of the ratings accordingly. For more information, please see [this article](http://www.moserware.com/assets/computing-your-skill/The%20Math%20Behind%20TrueSkill.pdf).

`results.py` defines the default parameters used for the TrueSkill algorithm and defines a function that applies TrueSkill to a list of rows of the `Results` table and updates the rows with the new ratings.

## Website
The website is a [Flask](https://flask.palletsprojects.com/en/1.1.x/) application deployed on [Heroku](https://www.heroku.com/) with a single user-facing webpage.

For troubleshooting, I set up the `/database` URL to display the first 2000 rows of each table in the database. Use the parameter `?table=Races` (e.g.) to query the desired table.

The Heroku app uses a production configuration (see `config.py`) which prevents users
from altering the database. In a development configuration, the following parameters can be used to alter the database using the `/database` URL:
- `drop`: if `True`, will drop all tables and re-create empty tables with the appropriate schema.
- `add`: if `True`, will attempt to add rows to the database from locally saved pandas DataFrames from the hard-coded locations on *my* computer. This is not meant for other users.
- `rate`: if `True`, will apply TrueSkill to all results in the database, regardless of whether the results have been rated already or not.
- `filter`: if `True`, removes races from the `Races` table that are not yet represented in the `Results` table. I used this while troubleshooting using only a subset of the data.

# Instructions for running locally
Details to be added...

Follow these steps to get the website running on your local machine:
1. `git clone` the repository
1. `pip install -r requirements.txt`
