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

    @dag(dag_id= 'amazing_scraper', schedule_interval='@daily', start_date=datetime(2021, 1, 1), catchup=False, tags=['scraping_project'])
    def taskflow():
        """
        ### TaskFlow API example using virtualenv
        This is a simple ETL data pipeline example which demonstrates the use of
        the TaskFlow API using three simple tasks for Extract, Transform, and Load.
        """

        @task.virtualenv(
            requirements=[
                'globalfirepower-scraper==0.0.2',
            ],
            python_version=3.9,
            system_site_packages=True
        )
        def extract():
            """
            #### Extract task
            Scrape Global Fire Power Countries Ranking in 2022.
            """
            from globalfirepower import GlobalFirePowerScraper

            scraper = GlobalFirePowerScraper()
            
            return scraper.get_armies_information().to_dict()

        @task.virtualenv(
            requirements=[
                'pandas'
            ],
            python_version=3.9,
            system_site_packages=True
        )
        def transform(ranking: dict):
            """
            #### Transform task
            Extract the variables we want and make some transformations
            """
            import pandas as pd

            ranking_df = pd.DataFrame.from_dict(ranking)
            ranking_df = ranking_df[[
                'rank',
                'name',
                'power_index',
                'progress',
                'Total Population',
                'Available Manpower',
                'Fit-for-Service',
                'Reaching Mil Age Annually',
                'Tot Military Personnel (est.)',
                'Active Personnel',
                'Reserve Personnel',
                'Paramilitary Total'
            ]].copy()

            ranking_df.rename(columns={
                'Total Population': 'total_population',
                'Available Manpower': 'available_manpower',
                'Fit-for-Service': 'fit_for_service',
                'Reaching Mil Age Annually': 'reaching_military_age_annually',
                'Tot Military Personnel (est.)': 'total_estimated_military_personnel',
                'Active Personnel': 'active_personnel',
                'Reserve Personnel': 'reserve_personnel',
                'Paramilitary Total': 'paramilitary_total'
            }, inplace=True)

            return ranking_df.to_dict()

        @task.virtualenv(
            requirements=[
                'pandas'
            ],
            python_version=3.9,
            system_site_packages=True
        )
        def load(countries_details: dict):
            """
            #### Load task
            Load the dictionary into BigQuery Dataset
            """
            import pandas as pd

            countries_details_df = pd.DataFrame.from_dict(countries_details)
            print(countries_details_df.head())

        countries_ranking = extract()
        countries_information = transform(countries_ranking)
        load(countries_information)

    dag = taskflow()
