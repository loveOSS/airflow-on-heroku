import logging
import shutil
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

if not shutil.which("virtualenv"):
    log.warning(
        "This Dag requires virtualenv, please install it."
    )
else:

    @dag(dag_id= 'amazing_scraper', schedule_interval='@daily', start_date=datetime(2021, 1, 1), catchup=False, tags=['scrapper'])
    def taskflow():
        """
        ### TaskFlow API example using virtualenv
        This is a simple ETL data pipeline example which demonstrates the use of
        the TaskFlow API using three simple tasks for Extract, Transform, and Load.
        """

        @task.virtualenv(
            requirements=[
                'globalfirepower-scraper==0.0.1',
            ],
            python_version=3.9,
            system_site_packages=False
        )
        def extract():
            """
            #### Extract task
            Scrape Global Fire Power Countries Ranking in 2022.
            """
            from globalfirepower import GlobalFirePowerScraper

            scraper = GlobalFirePowerScraper
            
            return scraper.get_country_ranking_table()

        @task.virtualenv(
            requirements=[
                'globalfirepower-scraper==0.0.1',
            ],
            python_version=3.9,
            system_site_packages=False
        )
        def transform(ranking):
            """
            #### Transform task
            Using the Countries Ranking table, get some details about
            each country and complete the data.
            """
            from globalfirepower import GlobalFirePowerScraper

            scraper = GlobalFirePowerScraper

            return scraper.get_countries_details(ranking)
            

        @task()
        def load(countries_details):
            """
            #### Load task
            Print the first lines of the Dataset
            """
            print(countries_details.head())

        countries_ranking = extract()
        countries_information = transform(countries_ranking)
        load(countries_information)

    dag = taskflow()
