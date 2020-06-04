import xml.sax
from collections import deque
from contextlib import suppress
import csv
from itertools import tee
import logging
import datetime
import os
import re
import sys
import queue
import threading
import concurrent.futures
from collections.abc import Mapping, Iterable

import boto3
import logging_gelf.handlers
import logging_gelf.formatters
from keboola import docker


class PricesHandler(xml.sax.ContentHandler):
    PRICE_ATTR_COLS_PREFIX = 'competitor_'

    def __init__(self, *, task_queue, columns_mapping, column_names, filedata=None):
        super().__init__()
        self.path = deque()
        self.current_row = {}
        self.current_content = []
        self.task_queue = task_queue
        self.columns_mapping = columns_mapping
        self.colnames = column_names

        self.competitor_attr_col_names = [
            cn[len(PricesHandler.PRICE_ATTR_COLS_PREFIX):]
            for cn
            in column_names
            if cn.startswith(PricesHandler.PRICE_ATTR_COLS_PREFIX)
        ]
        self.tag_col_names = [
            cn
            for cn
            in column_names
            if not cn.startswith(PricesHandler.PRICE_ATTR_COLS_PREFIX)
        ]

        self.filedata = filedata if filedata else {}

    def startElement(self, name, attrs):
        if attrs and name == 'price' and self.path[-1] == 'prices':
            self.current_row.update(
                {
                    f"{PricesHandler.PRICE_ATTR_COLS_PREFIX}{aname}": attrs.getValue(aname)
                    for aname
                    in attrs.getNames()
                    if aname in self.competitor_attr_col_names
                }
            )

        self.path.append(name)

    def endElement(self, name):
        self.path.pop()

        if name == 'product':
            # the current product is done -> reset all product info
            self.current_row = {}

        if name in self.tag_col_names:
            # join the parts of the current content into a single string and pass it to the current row if not empty
            value = ''.join(self.current_content).strip()
            if value:
                self.current_row[name] = value

        if name == 'price' and self.path[-1] == 'prices':
            # we have just finished reading a competitor info
            # -> write the result, and drop all the info pertaining to the current competitor
            # make sure that eshop name is properly formatted
            result = {**self.current_row, **self.filedata}
            result['competitor_eshop'] = clean_eshop_name(result.get('competitor_eshop', ''))

            # mall product ids are under one of the two tags
            result['MATERIAL'] = result.get('id') or result.get('in_user_1')

            # rename all we can, keep rest unchanged
            result = {self.columns_mapping.get(key, key): value for key, value in result.items()}
            self.task_queue.put(result)

            del self.current_row['price']
            for cname in self.competitor_attr_col_names:
                with suppress(KeyError):
                    del self.current_row[cname]

        self.current_content = []

    def characters(self, content):
        if not self.path:
            return

        if self.path[-1] in self.tag_col_names:
            # the content for a single tag sometimes comes in more than one segment for unclear reasons,
            # so we need to collect all of them and join later
            self.current_content.append(content)


def extract_country_from_filename(filename):
    filename_lower = filename.lower()
    country_lower = re.match('(mall-|pc-hf-scraping-materialy-)([a-z]{2})',
                             filename_lower).group(2)
    return country_lower.upper()


def clean_eshop_name(eshop):
    eshop_lower = eshop.lower()
    return eshop_lower.split('/')[0]


def extract_frequency(filename):
    return 'direct' if '-hf-' not in filename else 'hf'


class Producer:
    def __init__(self):
        self.utctime_started_datetime = datetime.datetime.utcnow()
        self.datadir = os.getenv("KBC_DATADIR", "/data/")
        cfg = docker.Config(self.datadir)
        self.parameters = cfg.get_parameters()
        # log parameters (excluding sensitive designated by '#')
        logging.info({k: v for k, v in self.parameters.items() if "#" not in k})
        self.wanted_columns = self.parameters.get("wanted_columns")
        self.columns_mapping = self.parameters.get("columns_mapping")
        self.allowed_file_patterns = self.parameters.get("allowed_file_patterns")
        self.forbidden_file_patterns = self.parameters.get("forbidden_file_patterns")
        self.last_timestamp_filename = self.parameters.get("last_timestamp_filename")
        self.input_filelist_filename = self.parameters.get("input_filelist_filename")
        self.input_fileset = {}
        self.files_to_process = []
        self.last_processed_timestamp = None
        self.max_timestamp_this_run_tz = None
        self.max_timestamp_this_run = None

    def read_last_processed_timestamp(self):

        with open(f'{self.datadir}in/tables/{self.last_timestamp_filename}.csv', 'r') as input_file:
            self.last_processed_timestamp = [
                str(ts.replace('"', ''))
                for ts
                # read all input file rows, except the header
                in input_file.read().split(os.linesep)[1:]
            ][0]

    def read_manually_added_files(self):

        if self.input_filelist_filename is not None:
            with open(f'{self.datadir}in/tables/{self.input_filelist_filename}.csv') as input_file:
                self.input_fileset = {
                    str(name.replace('"', ''))
                    for name
                    # read all input file rows, except the header
                    in input_file.read().split(os.linesep)[1:]
                }
            logging.info(f'Will try to download {len(self.input_fileset)} irrespective of timestamp.')

    def write_last_timestamp(self):
        # exit if there are no new files to process
        if self.max_timestamp_this_run_tz is None:
            logging.info("No new files to download. Putting DONE to queue.")
            self.max_timestamp_this_run = self.last_processed_timestamp

        else:
            self.max_timestamp_this_run = self.max_timestamp_this_run_tz.replace(
                tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

        with open(f"{self.datadir}out/tables/{self.last_timestamp_filename}.csv", 'w', encoding="utf-8") as f:
            dict_writer = csv.DictWriter(f, fieldnames=["max_timestamp_this_run"])
            dict_writer.writeheader()
            dict_writer.writerow({"max_timestamp_this_run": self.max_timestamp_this_run})

    def download_files_to_parse(self, task_queue):
        # connect to s3 bucket
        session = boto3.Session(aws_access_key_id=self.parameters.get("aws_key"),
                                aws_secret_access_key=self.parameters.get("#aws_secret"))
        s3 = session.resource("s3")
        esol_bucket = s3.Bucket(self.parameters.get("bucket_name"))

        # download xml files that are xml, were not present in the last download, have allowed pattern
        # or specifically enumerated files
        logging.info('Searching files to download.')
        files_to_download = (
            file
            for file
            in esol_bucket.objects.all()
            if (
                       file.key.endswith(".xml")
                       and any(name_pattern in file.key for name_pattern in self.allowed_file_patterns)
                       and all(name_pattern not in file.key for name_pattern in self.forbidden_file_patterns)
                       and file.last_modified.replace(tzinfo=None) >
                       datetime.datetime.strptime(self.last_processed_timestamp, "%Y-%m-%d %H:%M:%S")
               ) or (file.key in self.input_fileset)
        )
        # we need to reuse the generator
        # copy it to save memory
        files_to_download, files_to_download_backup = tee(files_to_download)

        self.max_timestamp_this_run_tz = max(
            (file.last_modified for file in files_to_download_backup),
            default=None
        )

        # we always need to write the timestamp even if there are no new files
        self.write_last_timestamp()

        if self.max_timestamp_this_run_tz is None:
            logging.info("No new files found")
            task_queue.put("DONE")
            sys.exit(0)

        logging.info("Collected files to download.")

        # create temp directory to store downloaded files
        if not os.path.exists(f'{self.datadir}downloaded_xmls'):
            os.makedirs(f'{self.datadir}downloaded_xmls')

        logging.info("Downloading files.")

        for file in files_to_download:
            filepath = f"{self.datadir}downloaded_xmls/" + file.key.split('/')[-1]
            self.files_to_process.append(filepath)
            with open(filepath, "wb") as f:
                esol_bucket.download_fileobj(file.key, f)
                logging.info(f"File {filepath.split('/')[-1]} downloaded.")

        del files_to_download, session, s3, esol_bucket

    def parse_files(self, task_queue):
        logging.info(f'Files to process {len(self.files_to_process)}')
        processed_files_count = 0
        for file in self.files_to_process:
            filename = file.split('/')[-1]

            logging.info(f"Processing file {filename}")

            frequency = extract_frequency(filename)
            country = extract_country_from_filename(filename)
            if frequency == 'hf':
                ts = self.utctime_started_datetime.strftime("%Y-%m-%d %H:%M:%S")
            else:
                # assuming that low-frequency data relate to prices at the beginning of the day
                # this helps prioritize records when daily data actually arrive later
                ts = self.utctime_started_datetime.strftime("%Y-%m-%d 00:00:00")

            h = PricesHandler(
                task_queue=task_queue,
                columns_mapping=self.columns_mapping,
                column_names=self.wanted_columns,
                filedata={'SOURCE': frequency, 'FREQ': 'd', 'SOURCE_ID': filename, 'TS': ts,
                          'COUNTRY': country}
            )
            xml.sax.parse(file, h)
            logging.info(f"File {filename} processing finished.")
            processed_files_count += 1
            logger.info(f"Processed: {processed_files_count} out of {len(self.files_to_process)}.")

        logging.info('All files enqueued. Putting DONE to queue.')
        task_queue.put('DONE')

    def produce(self, task_queue):
        self.read_last_processed_timestamp()
        self.read_manually_added_files()
        self.download_files_to_parse(task_queue)
        self.parse_files(task_queue)


def writer(task_queue, columns_list, threading_event, filepath):
    with open(filepath, 'w+') as outfile:
        results_writer = csv.DictWriter(outfile, fieldnames=columns_list, extrasaction='ignore')
        results_writer.writeheader()
        while not threading_event.is_set():
            chunk = task_queue.get()
            if chunk == 'DONE':
                logging.info('DONE received. Exiting.')
                threading_event.set()
            if isinstance(chunk, Mapping):
                results_writer.writerow(chunk)
            elif isinstance(chunk, Iterable):
                results_writer.writerows(chunk)
            else:
                logging.error(f'Chunk is neither Mapping, nor Iterable type. Chunk: {chunk}')
                logging.info('Skipping')


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    try:
        logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(host=os.getenv('KBC_LOGGER_ADDR'),
                                                                          port=int(os.getenv('KBC_LOGGER_PORT')))
        # remove stdout logging when running inside keboola
        logger.removeHandler(logger.handlers[0])
    except TypeError:
        logging_gelf_handler = logging.StreamHandler()

    logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    colnames = [
        "AVAILABILITY",
        "COUNTRY",
        "CSE_ID",
        "CSE_URL",
        "DISTRCHAN",
        "ESHOP",
        "FREQ",
        "HIGHLIGHTED_POSITION",
        "MATERIAL",
        "POSITION",
        "PRICE",
        "RATING",
        "REVIEW_COUNT",
        "SOURCE",
        "SOURCE_ID",
        "STOCK",
        "TOP",
        "TS",
        "URL",
    ]

    path = f'{os.getenv("KBC_DATADIR")}out/tables/results.csv'

    pipeline = queue.Queue(maxsize=1000)
    event = threading.Event()
    producer = Producer()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(producer.produce, pipeline)
        executor.submit(writer, pipeline, colnames, event, path)
