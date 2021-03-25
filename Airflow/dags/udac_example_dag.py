from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import (LoadFactOperator,
#                                LoadDimensionOperator, DataQualityOperator)
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

#########################################################################################################
#########################################################################################################
#########################################################################################################
######         The following DAG performs the following functions:                                  #####
######                                                                                              #####
###### 1. Loads categories, podcast and reviews from S3 to RedShift through StageToRedshiftOperator #####
###### 2. Uses the LoadDimensionOperator to create dimensions table in Redshift                     #####
###### 4. Performs a data quality check on the tables in RedShift                              #####
#########################################################################################################
#########################################################################################################
#########################################################################################################

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 3, 24),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay':300,
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_categories_to_redshift = StageToRedshiftOperator(
    task_id='stage_categories',
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials',
    table = 'staging_category',
    s3_bucket = 'podcast-project',
    s3_key = 'categories.csv',
    dag=dag
)

stage_podcast_to_redshift = StageToRedshiftOperator(
    task_id='stage_podcast',
    redshift_conn_id = 'redshift',
    table = 'staging_podcast',
    s3_bucket ='podcast-project',
    s3_key = 'podcast.csv',
    dag=dag
)

stage_reviews_to_redshift = StageToRedshiftOperator(
    task_id='stage_reviews',
    redshift_conn_id = 'redshift',
    table = 'staging_reviews',
    s3_bucket ='podcast-project',
    s3_key = 'reviews.csv',
    dag=dag
)

load_podcast_stats_table = LoadDimensionOperator(
    task_id='load_podcast_agg_table',
    redshift_conn_id = 'redshift',
    destination_table = 'podcast_agg_reviews',
    query_dimension = """  SELECT sp.podcast_id, sp.title
                     ,avg(sr.rating)
                      ,count(r.*)
                      FROM staging_podcast AS sp LEFT JOIN staging_reviews AS sr ON 
                      sp.podcast_id = sr.podcast_id
                      WHERE sr.rating IS NOT NULL 
                      GROUP BY sp.podcast_id, sp.title;""",
    dag=dag
)

load_categories_stats_table = LoadDimensionOperator(
    task_id='load_categories_agg_table',
    redshift_conn_id = 'redshift',
    destination_table = 'categories_agg_reviews',
    query_dimension = """(category, total_podcast, category_avg_rating)  
                      SELECT sc.category,count(distinct sc.podcast_id),avg(sr.rating)
                      FROM staging_category AS sc LEFT JOIN staging_reviews AS sr ON 
                      sc.podcast_id = sr.podcast_id
                      WHERE sr.rating IS NOT NULL
                      GROUP BY sc.category;""",
    dag=dag
)

load_podcast_dimension_table = LoadDimensionOperator(
    task_id='load_podcast_dim_table',
    redshift_conn_id = 'redshift',
    destination_table = 'podcast_dim',
    query_dimension = """SELECT sp.podcast_id, sp.title, sc.category, sp.itunes_url
                            FROM staging_podcast AS sp LEFT JOIN staging_category AS sc
                            ON sp.podcast_id = sc.podcast_id""",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_names = ['podcast_dim','podcast_agg_reviews','categories_agg_reviews']
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

##################################################
################# DAG STRUCTURE ##################
##################################################

start_operator >> [stage_categories_to_redshift, stage_podcast_to_redshift, stage_reviews_to_redshift]

stage_categories_to_redshift >> load_podcast_dimension_table
stage_podcast_to_redshift >> load_podcast_dimension_table
stage_reviews_to_redshift >> load_podcast_dimension_table

load_podcast_dimension_table >> [load_podcast_stats_table,load_categories_stats_table ]

load_podcast_stats_table >> run_quality_checks
load_categories_stats_table >> run_quality_checks

run_quality_checks >> end_operator
