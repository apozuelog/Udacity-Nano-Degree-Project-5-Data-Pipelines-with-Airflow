# DEND Data Pipelines with Airflow

### Introduction
Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Datasets
* Log data: s3://udacity-dend/log_data
* Song data: s3://udacity-dend/song_data

### Projects components
* /dags/udac_example_dag.py  
Contiene la implementación de los operadores:  
  - StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator  

Así como el orden de ejecución de las task:  
start_operator >> stage_events_to_redshift  
start_operator >> stage_songs_to_redshift  
stage_events_to_redshift >> load_songplays_table  
stage_songs_to_redshift >> load_songplays_table  
load_songplays_table >> load_user_dimension_table  
load_songplays_table >> load_song_dimension_table  
load_songplays_table >> load_artist_dimension_table  
load_songplays_table >> load_time_dimension_table  
load_user_dimension_table >> run_quality_checks  
load_song_dimension_table >> run_quality_checks  
load_artist_dimension_table >> run_quality_checks  
load_time_dimension_table >> run_quality_checks  
run_quality_checks >> end_operator  

* /plugins/helpers/sql_queries.py  
Contiene la clase 
  - SqlQueries 
    - songplay_table_insert
    - user_table_insert
    - song_table_insert
    - artist_table_insert
    - time_table_insert

* /plugins/operators/...
  * stage_redshift.py
    se encarga de cargar los archivos json a nuestras tablas staging_events y staging_songs de nuestro Redshift mediante el operador StageToRedshiftOperator
  * load_fact.py
  * load_dimension.py
  * data_quality.py
