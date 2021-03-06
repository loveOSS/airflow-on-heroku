# Install Apache Airflow on Heroku

## Install the project

```bash
git clone https://github.com/loveOSS/airflow-on-heroku.git
```

## Create the Heroku application

```bash
cd airflow-on-heroku
heroku apps:create
heroku addons:create heroku-postgresql
```

> For better performance and if you're willing to spend money, you can use Hobby-dev plan:

```bash
heroku addons:create heroku-postgresql:hobby-dev
```

## Set the Heroku Variables

```bash
cd airflow-on-heroku
heroku config:set SLUGIFY_USES_TEXT_UNIDECODE=yes
heroku config:set AIRFLOW_HOME=/app
heroku config:set AIRFLOW__CORE__DAGS_FOLDER="/app/dags"
heroku config:set AIRFLOW__CORE__LOAD_EXAMPLES=False
heroku config:set AIRFLOW__CORE__SQL_ALCHEMY_CONN=`heroku config:get DATABASE_URL`
heroku config:set AIRFLOW__CORE__EXECUTOR="LocalExecutor"
```

To set the fernet key for security:

```python
python contrib/create_key.py
```

And set the generated key in Heroku configuration:

```bash
heroku config:set AIRFLOW__CORE__FERNET_KEY="XXXXXXXXXXXXXXXXXXXXXXXXXX"
```

## Deploy on Heroku

```bash
cd airflow-on-heroku
rm -rf .git/
git init
git add .
git commit -m "First commit"
git checkout -b main
git branch -D master
git push heroku main
```
## Configure BigQuery Access

```bash
cd airflow-on-heroku

heroku config:set AMAZING_SCRAPER_BQ_CONN=<your_bigquery_airflow_connection_name>
heroku config:set AMAZING_SCRAPER_BQ_PROJECT=<your_bigquery_project_id>
heroku config:set AMAZING_SCRAPER_BQ_TABLE=<your_bigquery_dataset.table>
```

For exemple:

```bash
heroku config:set AMAZING_SCRAPER_BQ_CONN=my_bq_conn
heroku config:set AMAZING_SCRAPER_BQ_CONN=project-342414
heroku config:set AMAZING_SCRAPER_BQ_TABLE=globalfirepower_ranking.ranking
```

## Create Admin User account

```bash
cd airflow-on-heroku
heroku run bash

airflow users create \
    --username <username> \
    --firstname <Firstname> \
    --lastname <Lastname> \
    --role Admin \
    --email <your-email@exemple.com>

exit
```

## Access Apache Airflow Web UI

```bash
cd airflow-heroku
heroku open
```

## LICENSE

This project is provided under the MIT license.
