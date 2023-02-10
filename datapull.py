# coding: utf-8

# In[1]:

# import the necessary packages
import requests
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import pandas as pd
import numpy as np
import datetime as dt
import urllib
import urllib.request
import ftplib
import os
import sys
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2
import glob
import settings
import re
import shutil
from dateutil.relativedelta import relativedelta
from utilities_pull import pull_source_data, pull_source_data_finra, check_file_exist_in_target, clean_source_dir
from utilities import parse_filename, create_logger, get_source_file_name, send_email, process_notification, already_running, remove_temp_dir
import cx_Oracle
from metadata import MetadataJob
import getopt

if __name__ == "__main__":

    try:

        # get the current month and year
        today = dt.datetime.today()
        current_month = today.month
        current_year = today.year
        logger = create_logger('datapull')
        logfile_name = settings.LOGGER.handlers[0].baseFilename
        exception_found = False
        fdr = []
        list_success, list_error = [], []

        full_cmd_arguments = sys.argv
        logger.info('command line: ' + str(full_cmd_arguments[:]))

        log_out = settings.BASE_DIR + settings.LOG_DIR + "datapull_log_out.log"
        log_file = open(log_out, "w")  # overwrite existing file
        start_time = dt.datetime.now()
        trs = os.popen("hostname").read()

        p_background_print = True  # show print output in background, default True
        conETLMetadata, curETLMetadata = None, None
        opts, args = getopt.getopt(sys.argv[1:], 'hb:', ['help'])
        for opt, arg in opts:
            if opt in ('-h', '--help'):
                print('startup parameters:')
                print('data_scrape.py -b (print output to background)')  # -i <--initialize:False> -f <--datafix:True>
                sys.exit(1)
            elif opt in ("-b"):
                p_background_print = eval(arg)

        if p_background_print == True:
            sys.stdout = log_file

        fullCmdArguments = sys.argv

        logger.info('command line: ' + str(fullCmdArguments[:]))

        # check if process is already running
        if already_running(fullCmdArguments[0]):
            logger.exception('{0} is already running, exit the ETL.'.format(fullCmdArguments[0]))
            send_email('Data Pull ERROR -- Another process is running',
                       'Same name process is already running on this server. \n' +
                       'Please wait until the current one finished to start another process. Current process will exit.')
            sys.exit(1)

        # Oracle (metadata) connection
        conETLMetadata = cx_Oracle.connect(dsn=settings.DB_CONN_ETL, encoding="UTF-8", nencoding="UTF-8")
        curETLMetadata = conETLMetadata.cursor()

        year = current_year
        month = current_month
        dt_days_back = dt.datetime.now().date() - dt.timedelta(days=int(settings.CHECK_MISSING_FILES_DAYS))
        print('dt_days_back:', dt_days_back, '\n')

        # provide an error check to make sure that the data realistically exists
        # will only allow you to call data from two months prior to the current month
        if year > current_year:
            print('INVALID YEAR, MUST BE ' + str(current_year) + ' OR BEFORE')
        elif (year == current_year) & (month > current_month):
            print('INVALID MONTH, MUST BE ' + str(current_month) + ' OR BEFORE')
        else:
            print('current month/year:', str(month) + ' ' + str(year))

        #####################################################################################################
        # download data from website to input directory
        #####################################################################################################

        source_dir = settings.TEMP_DIR
        target_dir = settings.TARGET_DIR

        # loop through months to get the 60 day data
        first_day_of_month = pd.to_datetime('{}{}01'.format(str(year), str(month).zfill(2)), format='%Y%m%d')
        last_day_of_month = first_day_of_month + relativedelta(months=1) - relativedelta(days=1)
        print('last_day_of_month:', last_day_of_month, '\n')
        while last_day_of_month.date() >= dt_days_back:

            year = first_day_of_month.year
            month = first_day_of_month.month
            month_name = first_day_of_month.strftime('%B')  # i.e. October
            print(year, month, '\n')

            pull_source_data(year, month, 'xyz')

            first_day_of_month -= relativedelta(months=1)
            last_day_of_month = first_day_of_month + relativedelta(months=1) - relativedelta(days=1)

            # create landing year/month directory if not exist
            file_month = str(month).zfill(2)
            file_year = str(year)

        # get all files
        li_files_source = [os.path.basename(x) for x in glob.iglob('{}{}'.format(source_dir, '*.*'))]

        # get all files within 60 days
        li_files_source = [x for x in li_files_source if pd.to_datetime(re.findall('[0-9]{8}', x)[0], format='%Y%m%d').date() >= dt_days_back]  # exclude older files
        print('li_files_source:', li_files_source[:5], '\n')

        # compare source list against medata
        set_files_source = {x.split('.')[0] for x in li_files_source}  # set comprehension
        set_files_metadata = {x.split('.')[0] for x in get_source_file_name(curETLMetadata, dt_days_back, None, 'datapull')}
        set_files_filtered = set_files_source.difference(set_files_metadata)
        print('set_files_filtered len:', len(set_files_filtered), '\n')

        ##################################################################################################
        # check for new files comparing with target folder and move to target
        ##################################################################################################

        # move txt files to target
        fdr = [x for x in glob.iglob('{}{}'.format(source_dir, '*.*')) if os.path.basename(x).split('.')[0] in set_files_filtered]  # list_files_filtered_nameonly

        if len(fdr) == 0:
            send_email('data Datapull - NOTIFICATION', 'No new file to process.', logfile_name)

        for x in fdr:

            try:

                jobID, phaseID, fileID = None, None, None
                dataset_id, phase_id = None, None
                filename = os.path.basename(x)

                market_desc, source_file_date, dataset_id, source, tablename = parse_filename(filename, 'datapull')

                # instance for MetadataJob object
                metadata_job = MetadataJob(curETLMetadata, dataset_id, source_file_date, filename)

                # jobID
                jobID = metadata_job.jobID
                print('jobID:', jobID)

                # phaseID
                metadata_job.create_etl_job_track_phase('RETRIEVE_TO_LOCAL')

                file_date = pd.to_datetime(re.findall('[0-9]{8}', x)[0], format='%Y%m%d')  # daily file
                file_month = str(file_date.month).zfill(2)
                file_year = str(file_date.year)
                shutil.move(x, os.path.join(target_dir, os.path.basename(x)))

                metadata_job.create_etl_job_track_file()

                metadata_job.complete_etl_job_track_file(0, None)

                # complete phase
                metadata_job.complete_etl_job_track_phase()

                # process_log
                metadata_job.create_etl_process_log('INFO')

                # complete job
                metadata_job.complete_etl_job_track()

                list_success.append(filename)

            except Exception as e:

                logger.exception(e)
                print(e)
                list_error.append(filename)
                if metadata_job is not None:
                    metadata_job.error_etl_job_track_file()
                    metadata_job.error_etl_job_track_phase()
                    metadata_job.error_etl_job_track()
                    metadata_job.create_etl_process_log("ERROR")

        remove_temp_dir()

        end_time = dt.datetime.now()
        end_str = end_time - start_time

        print("""
start_time=""" + start_time.strftime("%Y-%m-%d %H:%M:%S") + """, end_time=""" + end_time.strftime("%Y-%m-%d %H:%M:%S") + """
Time of completion=""" + str(end_str), '\n')

    except Exception as e:
        print(e)
        logger.exception(e)
        send_email('data Datapull - Error', str(e), logfile_name)
        exception_found = True

    finally:

        if curETLMetadata is not None:
            curETLMetadata.close()
        if conETLMetadata is not None:
            conETLMetadata.close()

        process_notification('datapull', list_success, list_error, logfile_name)

        log_file.close()
