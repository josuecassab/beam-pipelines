#! /bin/bash

# Topic y subscription creation command line arguments
gcloud pubsub topics create test
gcloud pubsub subscriptions create test-subs --topic=test

# data generator command line arguments
python generator.py --project-id myproject-387020 --topic test

# pubsub_to_bigquery command line arguments
python pubsub_to_bigquery.py --streaming --project-id myproject-387020 --subscription test-subs --dataset tweeper --table tweeps

# s3_to_bigquery command lines arguments
# S3_path = s3://josue-oscars-dataset/oscar.csv
python s3_to_bigquery.py --path oscar.csv --table myproject-387020.oscar_dataset.oscar --temp_location gs://myproject-387020-josue/temp