# BikeRank: skill ratings for amateur road cyclists

This is a **working draft** of an application created by [Brian Schaefer](https://www.linkedin.com/in/briantschaefer/) for [The Data Incubator](https://www.thedataincubator.com/) (Fall 2020 cohort).
This project will be finalized by **December 1, 2020**. Until then, please excuse any shortcomings.

BikeRank adopts the [TrueSkill™ Ranking System](https://www.microsoft.com/en-us/research/project/trueskill-ranking-system/) to [amateur road cycling races](https://results.bikereg.com/).
This project will eventually be viewable online at: http://bike-rank.herokuapp.com/. Currently, the time required to load the page (which involves querying an AWS-hosted database) is longer than the 30-second Heroku timeout. I am working diligently to fix this issue.

# Architecture
This project combines an assortment of techniques new to the author, each explained briefly below.

## Web scraping
Results for over 12,000 bike races are available at URLs like https://results.bikereg.com/race/11456,
where the race ID number ranges from 1-12649 (as of November 2020).
I use [`requests_futures`](https://pypi.org/project/requests-futures/) to asynchronously obtain the text of these webpages (`scraping.get_futures`) and use [regular expressions](https://docs.python.org/3/library/re.html) to extract the name, date, and location for each race (`scraping.scrape_race_page`).
Hidden within each of these pages is a link to a JSON file containing the results for that race.
I again use `requests_futures` to download the contents of these JSON files (`scraping.get_results_futures`) and convert them into Python dictionaries (`scraping.scrape_results_json`).

## Database
The relevant data for this project are stored in a PostgreSQL database hosted on [AWS](https://aws.amazon.com/rds/) with three tables:
- `Races`: Each row corresponds to one race event identified by a unique `race_id`. This table stores the relevant metadata for each race, including its name, date, location, list of race categories, and the number of racers competing in each category.
- `Racers`: Each row corresponds to one racer identified by a unique `RacerID`. This table primarily stores the racer's name and current skill rating (parameterized by a mean skill rating `mu` and uncertainty `sigma`). The ratings in this table are updated upon processing each additional race.
- `Results`: Each row corresponds to one result: the finishing place for one racer in one category of one race, along with the corresponding ID numbers for each. This table also records the skill rating of each racer both prior to (`prior_mu`, `prior_sigma`) and as a result of (`mu`, `sigma`) the race outcome.

In `model.py`, I represent the tables using [SQLAlchemy](https://docs.sqlalchemy.org/en/13/orm/tutorial.html) classes. There are a variety of helper functions defined here to query and update the database.

## TrueSkill
I have adapted the Python implementation of [TrueSkill](https://trueskill.org/) to determine skill ratings for the racers represented in the dataset. This algorithm compares the skill ratings of racers involved in each race and evaluates the final results considering its prior knowledge of each racer's relative skill. For more information about how the algorithm works, please see [this article](http://www.moserware.com/assets/computing-your-skill/The%20Math%20Behind%20TrueSkill.pdf).

While the mathematics behind TrueSkill are relatively complex, updating ratings is straightforward: TrueSkill receives a list of the skill ratings as input and returns a list of updated ratings.
`results.get_all_ratings` iterates through each category of each race in chronological order and applies TrueSkill (`results.run_trueskill`) to all placing racers. `results.get_predicted_places` predicts the finishing place for a group of racers by ordering their ratings - the racer with the highest rating is predicted to finish in 1st place, and so on.

## Website
The website is a [Flask](https://flask.palletsprojects.com/en/1.1.x/) application deployed on [Heroku](https://www.heroku.com/) with a single user-facing webpage.

For troubleshooting, I set up the `/database` URL to display the first 2000 rows of each table in the database. The parameters `table` and `start` can be used to specify which table to query and from what index to start showing results (e.g. `?table=Races&start=23`). If the `table` parameter is not specified, the page displays the `Results` table, and if the `start` parameter is not specified, the rows start from index 0.

The Heroku app uses a production configuration (see `config.py`) which prevents users
from altering the database. In a development configuration, the following parameters can be used to alter the database using the `/database` URL:
- `drop`: either `True` or comma-separated table names (e.g. `Races,Results`). Will drop listed tables (all tables if `True`) and re-create empty tables with the appropriate schema, using the functions `commands.db_drop_all` and `commands.db_create_all`.
- `add`: either `True` or comma-separated table names (e.g. `Races,Results`). Will attempt to add rows to the listed tables (all tables if `True`) by scraping each BikeReg race page and/or results JSON. This parameter calls the `add_table` method for each table.
- `subset`: two comma-separated integers (e.g. `subset=1,1000`) indicating the range of `race_id`s to add to the `Races` table. If not specified, the range of `race_id`s will be `1,13000`.
- `rate`: if `True`, will apply TrueSkill to all results in the database, regardless of whether the results have been rated already or not.
- `limit`: integer specifying the number of `Results` rows to rate, for debugging purposes.

# Instructions for running locally
Follow these steps to get the website running on your local machine:
1. `git clone` the repository
1. `pip install -r requirements.txt`
1. [Install](https://www.postgresql.org/download/) PostgreSQL and [create a database](https://www.tutorialspoint.com/postgresql/postgresql_create_database.htm).
1. Create a `.env` file in the root directory of the project with the following contents:
```
APP_SETTINGS=config.DevelopmentConfig
DATABASE_URL=postgres://<username>:<password>@<host>:<port>/<db_name>
SECRET_KEY=<secret_key_here>
```
1. Execute `flask db-create-all` to create all tables in the database (execute `flask db-drop-all` first if tables already exist in the database).
1. Run the Flask app with `flask run`.
1. Navigate to `localhost:5000/database`. At this point, the `Results` table is empty, so you should only see the column names.
1. Navigate to `localhost:5000/database?add=True&subset=1,1000` to add data to (in order) the `Races`, `Results`, and `Racers` tables. As explained above, the `subset` parameter (optional) can be used to limit the number of races considered and should be omitted to add the entire dataset.
1. Finally, navigate to `localhost:5000`. If the default race and racer are not in the database yet, the home page will display a random race and a random racer that participated in that race.
