# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""storage-to-dataflow-bigquery.py is a Dataflow pipeline which reads a file and writes its 
contents to a BigQuery table.
 
"""

from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class DataIngestion:
    """A helper class which contains the logic to translate the file into 
    a format BigQuery will accept."""

    def parse_method(self, string_input):
        # Strip out return characters and quote characters.
        values = re.split(";",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))

        row = dict( zip(('load_timestamp', 'ip', 'visit_id', 'device_type', 'url_location', 'page_type', 'search_query', 'product_id', 'site_department_id', 'product_unit_price', 'freight_delivery_time', 'freight_value', 'cart_qty', 'cart_total_value'),
                values))

        return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.
    # Specifically we have the input file to load and the output table to
    # This is the final stage of the pipeline, where we define the destination
    # of the data.  In this case we are writing to BigQuery.
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        # This example file contains a total of only 10 lines.
        # Useful for developing on a small set of data
        default='gs://seu-projeto-nome-no-google-bucket-navigation/dados_navegacionais*')
    # This defaults to the lake dataset in your bigquery project.  You'll have
    # to create the lake dataset yourself using this command:
    # bq mk lake

    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='seu-projeto-nome-no-google:dataNavigationDataSet.RAW_DATA_NAVIGATION')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line.  This includes information including where Dataflow should
    # store temp files, and what the project id is
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    (p
     # Read the file.  This is the source of the pipeline.  All further
     # processing starts with lines read from the file.  We use the input
     # argument from the command line.  We also skip the first line which is a
     # header row.
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written.  This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
     | 'String To BigQuery Row' >> beam.Map(lambda s:
                                            data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output,
            # Here we use the simplest way of defining a schema:
            # fieldName:fieldType
            # schema='load_timestamp:TIMESTAMP,ip:STRING,visit_id:STRING,device_type:STRING,url_location:STRING,page_type:STRING,search_query:STRING,product_id:INTEGER,site_department_id:INTEGER,product_unit_price:FLOAT,freight_delivery_time:FLOAT,freight_value:FLOAT,cart_qty:FLOAT,cart_total_value:FLOAT',
			schema='load_timestamp:STRING,ip:STRING,visit_id:STRING,device_type:STRING,url_location:STRING,page_type:STRING,search_query:STRING,product_id:STRING,site_department_id:STRING,product_unit_price:STRING,freight_delivery_time:STRING,freight_value:STRING,cart_qty:STRING,cart_total_value:STRING',
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()