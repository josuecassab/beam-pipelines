import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import re
from typing import NamedTuple
import argparse

SCHEMA = 'Number:STRING,Film:STRING,Year:STRING,Award:STRING,Nomination:STRING'
schema = ['Number', 'Film', 'Year', 'Award', 'Nomination']
def dic_convertion(schema: list, row: list):
    res = {}
    for x, y in zip(schema, row):
        res[x] = y
    return res

class Movies(NamedTuple):
    Number:int
    Film:str
    Year:int
    Award:int
    Nomination:int

if __name__ == '__main__':
   
    parser = argparse.ArgumentParser()
    parser.add_argument('--path')
    parser.add_argument('--table')
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
   
with beam.Pipeline(options=pipeline_options) as pipe:
        output = (
            pipe 
            | 'ReadData' >>  beam.io.ReadFromText(known_args.path, skip_header_lines=1)
            | "Split data" >> beam.Map(lambda x: re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', x)).with_output_types(Movies)
            | 'transform into dictionary' >> beam.Map(lambda item: dic_convertion(schema, item))
            #| 'transform into dictionary' >> beam.Map(lambda item: beam.Row(Number=item[0], Film=item[1], Year=item[2], Award=item[3], Nomination=item[4]))
            | "Insert records" >> beam.io.WriteToBigQuery(
                                                        table=known_args.table,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                        schema=SCHEMA
                                                        )    
       )
       









#temp_location='gs://myproject-387020-josue/temp', flexrs_goal="SPEED_OPTIMIZED"
#options = PipelineOptions([ "--project", "myproject-387020",
#                            "--job_name", "oscar",
#                            "--temp_location", "gs://myproject-387020-josue/temp", 
#                            "--staging_location", "gs://myproject-387020-josue/staging", 
#                            "--flexrs_goal", 'COST_OPTIMIZED'])