from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

import apache_beam as beam
import argparse


def parse_pubsub(data):
    import json
    return json.loads(data)


def fix_timestamp(data):
    import datetime
    d = datetime.datetime.strptime(data['created_at'], "%d/%b/%Y:%H:%M:%S")
    data['created_at'] = d.strftime("%Y-%m-%d %H:%M:%S")
    return data


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--project-id', required=True)
    parser.add_argument('--subscription', required=True)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--table', required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)

    SUBSCRIPTION = 'projects/' + known_args.project_id + '/subscriptions/' + known_args.subscription
    SCHEMA = 'created_at:TIMESTAMP,tweep_id:STRING,text:STRING,user:STRING'

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p | 'ReadData' >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION).with_output_types(bytes)
       | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
       | 'PubSubToJSON' >> beam.Map(parse_pubsub)
       #| 'Print' >> beam.Map(print)
       | 'FixTimestamp' >> beam.Map(fix_timestamp)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           f'{known_args.project_id}:{known_args.dataset}.{known_args.table}',
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
           )
       )
    result = p.run()
    result.wait_until_finish()
