import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
import json
import logging

logging.basicConfig(level=logging.INFO)

class SumFields(beam.DoFn):
    def process(self, element):
        items = element[1]
        final_value = {}
        for item in items:
            final_value['total'] = (item['quantity'] * item['price']) + final_value.get('total', 0)
            final_value.update({key: val for key, val in item.items() if key not in ['quantity', 'price']})
        yield final_value

def log_info(element):
    logging.info(element)
    logging.info(type(element))
    return element

def create_tuple(element):
    return element[0], element[1]

def main():

    with open('config.json') as f:
        config = json.load(f)

    beam_options = PipelineOptions(runner="DirectRunner")

    with beam.Pipeline(options=beam_options) as p:

        msgs = p | ReadFromKafka(
                                config,
                                ['source'],
                                start_read_time=0,
                                max_num_records=7,
                                )
        
        decoded = msgs | beam.Map(lambda x: x[1])
        transformed = ( decoded | "decode" >> beam.Map(lambda x: x.decode('utf-8'))
                                | "convert into dict" >> beam.Map(lambda x: json.loads(x))
                                | beam.Map(lambda element: beam.window.TimestampedValue(element, element['timestamp']))
                                | "window" >> beam.WindowInto(beam.window.FixedWindows(60))
                                | "Create Key" >> beam.Map(lambda element: (element['id'], element))
                                | "GroupByKey" >> beam.GroupByKey()
                                | "Sum fields" >> beam.ParDo(SumFields())
                                | "convert into tuple" >> beam.Map(lambda x: (str(x['id']).encode('utf-8'), json.dumps(x).encode('utf-8')))
                                | "logging 1" >> beam.Map(log_info).with_output_types(tuple[bytes, bytes])
                            )
        transformed | "write to kafka" >> WriteToKafka(producer_config=config, topic='destination')


if __name__ == '__main__':
    main()