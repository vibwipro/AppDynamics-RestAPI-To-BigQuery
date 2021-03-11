#****************************************************************
#Script Name    : BQ_Load_60Min.py
#Description    : This script will load data in BQ table.
#Created by     : Vibhor Gupta
#Version        Author          Created Date    Comments
#1.0            Vibhor          2020-07-29      Initial version
#****************************************************************

#------------Import Lib-----------------------#
import apache_beam as beam
from apache_beam import window
#from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os, sys, time
#from apache_beam.runners.dataflow.ptransform_overrides import CreatePTransformOverride
#from apache_beam.runners.dataflow.ptransform_overrides import ReadPTransformOverride
import argparse
import logging
from apache_beam.options.pipeline_options import SetupOptions

#------------Set up BQ parameters-----------------------#
# Replace with Project Id
project = 'PROJ'

#plitting Of Records----------------------#
class Transaction(beam.DoFn):
    def process(self, element):
        logging.info(element)
        return [{"Datetime_startTimeInMillis": time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(int(element[12]) / 1000)),"service_name": element[0],"metricId": element[1],"frequency": element[2],"useRange": element[3], "count": int(element[4]), "min": int(element[5]), "max": int(element[6]), "sum": element[7], "standardDeviation": int(element[8]), "value": int(element[9]), "current": int(element[10]), "occurrences": int(element[11]), "startTimeInMillis": element[12]}]


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
          '--input',
          dest='input',
          help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p1 = beam.Pipeline(options=pipeline_options)



    #data_f = sys.argv[1]
    logging.info('***********')
    logging.info(known_args.input)
    data_loading = (
        p1
        |'Read from File' >> beam.io.ReadFromText(known_args.input,skip_header_lines=0)
        |'Spliting of Fields' >> beam.Map(lambda record: record.split(','))
    )


    project_id = "PROJ"
    dataset_id = 'Prod_AppDynamics'
    table_schema = ('Datetime_startTimeInMillis:DATETIME, service_name:STRING, metricId:INTEGER, frequency:STRING, useRange:STRING, count:INTEGER, min:INTEGER, max:INTEGER, sum:STRING, standardDeviation:INTEGER, value:INTEGER, current:INTEGER, occurrences:INTEGER, startTimeInMillis:STRING')

##########################################################################################################################################
##------------------------Loading mBuy_ITDBFEMJBCTAXINVOICE01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-TAXINVOICE01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCTAXINVOICE01')
        | 'Clean-TAXINVOICE01' >> beam.ParDo(Transaction())
        | 'Write-TAXINVOICE01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCTAXINVOICE01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCTAXINVOICE01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-TAXINVOICE01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCTAXINVOICE01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-TAXINVOICE01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-TAXINVOICE01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCTAXINVOICE01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCTAXINVOICE01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-TAXINVOICE01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCTAXINVOICE01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-TAXINVOICE01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-TAXINVOICE01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCTAXINVOICE01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-----------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCSERVICE01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-SERVICE01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCSERVICE01')
        | 'Clean-SERVICE01' >> beam.ParDo(Transaction())
        | 'Write-SERVICE01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCSERVICE01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCSERVICE01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-SERVICE01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCSERVICE01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-SERVICE01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-SERVICE01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCSERVICE01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCSERVICE01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-SERVICE01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCSERVICE01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-SERVICE01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-SERVICE01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCSERVICE01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-----------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSEVENT01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSEVENT01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSEVENT01')
        | 'Clean-MFSEVENT01' >> beam.ParDo(Transaction())
        | 'Write-MFSEVENT01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSEVENT01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSEVENT01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSEVENT01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSEVENT01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSEVENT01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-MFSEVENT01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSEVENT01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSEVENT01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSEVENT01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSEVENT01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSEVENT01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-MFSEVENT01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSEVENT01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-----------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSDSUP01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSDSUP01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSDSUP01')
        | 'Clean-MFSDSUP01' >> beam.ParDo(Transaction())
        | 'Write-MFSDSUP01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSDSUP01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSDSUP01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSDSUP01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSDSUP01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSDSUP01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-MFSDSUP01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSDSUP01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSDSUP01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSDSUP01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSDSUP01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSDSUP01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-MFSDSUP01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSDSUP01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#------------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSCUST01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSCUST01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSCUST01')
        | 'Clean-MFSCUST01' >> beam.ParDo(Transaction())
        | 'Write-MFSCUST01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSCUST01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSCUST01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSCUST01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSCUST01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSCUST01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-MFSCUST01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSCUST01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSCUST01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSCUST01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSCUST01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSCUST01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-MFSCUST01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSCUST01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-----------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSCOM01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSCOM01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSCOM01')
        | 'Clean-MFSCOM01' >> beam.ParDo(Transaction())
        | 'Write-MFSCOM01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSCOM01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSCOM01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSCOM01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSCOM01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSCOM01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-MFSCOM01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSCOM01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSCOM01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSCOM01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSCOM01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSCOM01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-MFSCOM01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSCOM01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-----------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSPAYM01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSPAYM01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSPAYM01')
        | 'Clean-MFSPAYM01' >> beam.ParDo(Transaction())
        | 'Write-MFSPAYM01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSPAYM01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSPAYM01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSPAYM01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSPAYM01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSPAYM01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-MFSPAYM01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSPAYM01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSPAYM01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSPAYM01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSPAYM01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSPAYM01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-MFSPAYM01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSPAYM01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-----------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSRANGE01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSRANGE01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSRANGE01')
        | 'Clean-MFSRANGE01' >> beam.ParDo(Transaction())
        | 'Write-MFSRANGE01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSRANGE01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSRANGE01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSRANGE01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSRANGE01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSRANGE01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-MFSRANGE01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSRANGE01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSRANGE01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSRANGE01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSRANGE01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSRANGE01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-MFSRANGE01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSRANGE01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-----------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSRANGECACHEA01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSRANGECACHEA01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSRANGECACHEA01')
        | 'Clean-MFSRANGECACHEA01' >> beam.ParDo(Transaction())
        | 'Write-MFSRANGECACHEA01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSRANGECACHEA01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSRANGECACHEA01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSRANGECACHEA01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSRANGECACHEA01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSRANGECACHEA01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-MFSRANGECACHEA01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSRANGECACHEA01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSRANGECACHEA01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSRANGECACHEA01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSRANGECACHEA01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSRANGECACHEA01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-MFSRANGECACHEA01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSRANGECACHEA01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-----------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSRANGECACHEB01 service ----------------------------------------------------------------
    result = (
        data_loading 
        | 'Filter-MFSRANGECACHEB01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSRANGECACHEB01')
        | 'Clean-MFSRANGECACHEB01' >> beam.ParDo(Transaction())
        | 'Write-MFSRANGECACHEB01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSRANGECACHEB01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSRANGECACHEB01---- ITC1241  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSRANGECACHEB01-ITC1241' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSRANGECACHEB01%7CITC1241: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSRANGECACHEB01-ITC1241' >> beam.ParDo(Transaction())
        | 'Write-MFSRANGECACHEB01-ITC1241' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSRANGECACHEB01_ITC1241',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSRANGECACHEB01----ITC1242 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSRANGECACHEB01-ITC1242' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSRANGECACHEB01%7CITC1242: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSRANGECACHEB01-ITC1242' >> beam.ParDo(Transaction())
        | 'Write-MFSRANGECACHEB01-ITC1242' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSRANGECACHEB01_ITC1242',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-------------------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01_BATCH service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01-BATCH' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01_BATCH')
        | 'Clean-MFSSALES01-BATCH' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01-BATCH' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01_BATCH',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01_BATCH---- ITC1251  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01-BATCH-ITC1251' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01_BATCH%7CITC1251: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSSALES01-BATCH-ITC1251' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01-BATCH-ITC1251' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01_BATCH_ITC1251',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01_BATCH----ITC1252 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01-BATCH-ITC1252' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01_BATCH%7CITC1252: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSSALES01-BATCH-ITC1252' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01-BATCH-ITC1252' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01_BATCH_ITC1252',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01_BATCH---- ITC1253  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01-BATCH-ITC1253' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01_BATCH%7CITC1253: itbosski-lx2991.proj.com: 1521')
        | 'Clean-MFSSALES01-BATCH-ITC1253' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01-BATCH-ITC1253' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01_BATCH_ITC1253',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01_BATCH----ITC1254 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01-BATCH-ITC1254' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01_BATCH%7CITC1254: itbosski-lx2992.proj.com: 1521')
        | 'Clean-MFSSALES01-BATCH-ITC1254' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01-BATCH-ITC1254' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01_BATCH_ITC1254',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-------------------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01')
        | 'Clean-MFSSALES01' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01---- ITC1251  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01-ITC1251' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01%7CITC1251: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSSALES01-ITC1251' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01-ITC1251' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01_ITC1251',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01----ITC1252 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01-ITC1252' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01%7CITC1252: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSSALES01-ITC1252' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01-ITC1252' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01_ITC1252',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01---- ITC1253  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01-ITC1253' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01%7CITC1253: itbosski-lx2991.proj.com: 1521')
        | 'Clean-MFSSALES01-ITC1253' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01-ITC1253' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01_ITC1253',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALES01----ITC1254 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALES01-ITC1254' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALES01%7CITC1254: itbosski-lx2992.proj.com: 1521')
        | 'Clean-MFSSALES01-ITC1254' >> beam.ParDo(Transaction())
        | 'Write-MFSSALES01-ITC1254' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALES01_ITC1254',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-------------------------------------------------------------------------------------------------------------------------------------------------
##------------------------Loading mBuy_ITDBFEMJBCMFSSALESMAINT01 service ----------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALESMAINT01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALESMAINT01')
        | 'Clean-MFSSALESMAINT01' >> beam.ParDo(Transaction())
        | 'Write-MFSSALESMAINT01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALESMAINT01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALESMAINT01---- ITC1261  service -------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALESMAINT01-ITC1261' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALESMAINT01%7CITC1261: itbosski-lx1234.proj.com: 1521')
        | 'Clean-MFSSALESMAINT01-ITC1261' >> beam.ParDo(Transaction())
        | 'Write-MFSSALESMAINT01-ITC1261' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALESMAINT01_ITC1261',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
##------------------------Loading mBuy_ITDBFEMJBCMFSSALESMAINT01----ITC1262 service ------------------------------------------------------
    result = (
        data_loading
        | 'Filter-MFSSALESMAINT01-ITC1262' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBFEMJBCMFSSALESMAINT01%7CITC1262: itbosski-lx2917.proj.com: 1521')
        | 'Clean-MFSSALESMAINT01-ITC1262' >> beam.ParDo(Transaction())
        | 'Write-MFSSALESMAINT01-ITC1262' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBFEMJBCMFSSALESMAINT01_ITC1262',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------mBuy-ITDBSRUEBCMFSSALES01-------------------------------------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-RU-MFSSALES01' >> beam.Filter(lambda record: record[0] == 'mBuy-ITDBSRUEBCMFSSALES01')
        | 'Clean-RU-MFSSALES01' >> beam.ParDo(Transaction())
        | 'Write-RU-MFSSALES01' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBSRUEBCMFSSALES01',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------ISOM-ITAPSOM-------------------------------------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-ITAPSOM' >> beam.Filter(lambda record: record[0] == 'ISOM-ITAPSOM')
        | 'Clean-ITAPSOM' >> beam.ParDo(Transaction())
        | 'Write-ITAPSOM' >> beam.io.WriteToBigQuery(
                                                    table='ISOM_ITAPSOM',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------ISOM-ITEUSOM-------------------------------------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-ITEUSOM' >> beam.Filter(lambda record: record[0] == 'ISOM-ITEUSOM12c')
        | 'Clean-ITEUSOM' >> beam.ParDo(Transaction())
        | 'Write-ITEUSOM' >> beam.io.WriteToBigQuery(
                                                    table='ISOM_ITEUSOM',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------ISOM-ITCNSOM-------------------------------------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-ITCNSOM' >> beam.Filter(lambda record: record[0] == 'ISOM-ITCNSOM')
        | 'Clean-ITCNSOM' >> beam.ParDo(Transaction())
        | 'Write-ITCNSOM' >> beam.io.WriteToBigQuery(
                                                    table='ISOM_ITCNSOM',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-------------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------ISOM-ITNASOM-------------------------------------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-ITNASOM' >> beam.Filter(lambda record: record[0] == 'ISOM-ITNASOM')
        | 'Clean-ITNASOM' >> beam.ParDo(Transaction())
        | 'Write-ITNASOM' >> beam.io.WriteToBigQuery(
                                                    table='ISOM_ITNASOM',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#-------------------------------------------------------------------------------------------------------------------------------------------------
#---------------------------mBuy- ITDBSEBCIMFSSALES02--------------------------------------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-BCIMFSSALES02' >> beam.Filter(lambda record: record[0] == 'mBuy- ITDBSEBCIMFSSALES02')
        | 'Clean-BCIMFSSALES02' >> beam.ParDo(Transaction())
        | 'Write-BCIMFSSALES02' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBSEBCIMFSSALES02',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#---------------------------mBuy- ITDBSEBCIMFSSALES02 ITC1271-------------------------------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-BCIMFSSALES02-ITC1271' >> beam.Filter(lambda record: record[0] == 'mBuy- ITDBSEBCIMFSSALES02%7CITC1271: itbosski-lx2924.proj.com: 1521')
        | 'Clean-BCIMFSSALES02-ITC1271' >> beam.ParDo(Transaction())
        | 'Write-BCIMFSSALES02-ITC1271' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBSEBCIMFSSALES02_ITC1271',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))
#---------------------------mBuy- ITDBSEBCIMFSSALES02- ITC1272----------------------------------------------------------------------------------------
    result = (
        data_loading
        | 'Filter-BCIMFSSALES02-ITC1272' >> beam.Filter(lambda record: record[0] == 'mBuy- ITDBSEBCIMFSSALES02%7CITC1272: itbosski-lx2925.proj.com: 1521')
        | 'Clean-BCIMFSSALES02-ITC1272' >> beam.ParDo(Transaction())
        | 'Write-BCIMFSSALES02-ITC1272' >> beam.io.WriteToBigQuery(
                                                    table='mBuy_ITDBSEBCIMFSSALES02_ITC1272',
                                                    dataset=dataset_id,
                                                    project=project_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                    ))


#-------------------------------End -------------------------------------------------------------------------------------------------------------
    result = p1.run()
    result.wait_until_finish()


if __name__ == '__main__':
  #logging.getLogger().setLevel(logging.INFO)
  path_service_account = '/home/vibhg/PROJ-fbf8cf2fc927.json'
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account
  run()
