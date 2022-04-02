import logging
import shutil
from datetime import datetime

import google.cloud.bigquery as bq
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

log = logging.getLogger(__name__)

if not shutil.which("virtualenv"):
    log.warning("This Dag requires virtualenv, please install it.")
else:

    @dag(
        dag_id="amazing_scraper",
        schedule_interval="@daily",
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["scraping_project"],
    )
    def taskflow():
        """
        ### Scrape Global FirePower Workflow
        1. Scrape the Website
        2. Transform the data
        3. Load to BigQuery
        """

        @task.virtualenv(
            requirements=[
                "globalfirepower-scraper==0.0.2",
            ],
            python_version=3.9,
            system_site_packages=True,
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
            requirements=["pandas"],
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
            ranking_df = ranking_df[
                [
                    "rank",
                    "name",
                    "power_index",
                    "progress",
                    "Total Population",
                    "Available Manpower",
                    "Fit-for-Service",
                    "Reaching Mil Age Annually",
                    "Tot Military Personnel (est.)",
                    "Active Personnel",
                    "Reserve Personnel",
                    "Paramilitary Total",
                ]
            ].copy()

            ranking_df.rename(
                columns={
                    "Total Population": "total_population",
                    "Available Manpower": "available_manpower",
                    "Fit-for-Service": "fit_for_service",
                    "Reaching Mil Age Annually": "reaching_military_age_annually",
                    "Tot Military Personnel (est.)": "total_estimated_military_personnel",
                    "Active Personnel": "active_personnel",
                    "Reserve Personnel": "reserve_personnel",
                    "Paramilitary Total": "paramilitary_total",
                },
                inplace=True,
            )

            return ranking_df.to_dict()

        @task
        def load(countries_details: dict):
            """
            #### Load task
            Load the dictionary into BigQuery Dataset
            """
            import pandas as pd
            import os

            countries_details_df = pd.DataFrame(countries_details)

            # Connect to the right Airflow connection using BigQueryHook
            bigquery_hook = BigQueryHook(
                bigquery_conn_id=os.get("AMAZING_SCRAPER_BQ_CONN")
            )

            # Setup the BigQuery Client using BigQueryHook credentials
            bigquery_client = bq.Client(
                project=os.get("AMAZING_SCRAPER_BQ_PROJECT"),
                credentials=bigquery_hook._get_credentials(),
            )

            # By default, the loading operation appends data.
            job_config = bq.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

            # Start the loading
            job = bigquery_client.load_table_from_dataframe(
                countries_details_df,
                os.get("AMAZING_SCRAPER_BQ_TABLE"),
                job_config=job_config,
            )

            job.result()

        countries_ranking = extract()
        countries_information = transform(countries_ranking)
        load(countries_information)

    dag = taskflow()
