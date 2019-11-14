from keboola import docker
import xml.sax
from collections import deque
from contextlib import suppress
from csv import DictWriter
from itertools import tee
import logging
import datetime
import os
import boto3


class PricesHandler(xml.sax.ContentHandler):
    PRICE_ATTR_NAMES = [
        'url', 'eshop', 'product-name', 'product-percent-match', 'stock', 'availability',
        'assignement-status', 'checked-status',
    ]

    def __init__(self, writer, filedata={}):
        super().__init__()
        self.path = deque()
        self.current_row = dict()
        self.writer = writer
        self.filedata = filedata

    def startElement(self, name, attrs):
        if name in ['price-check', 'date', 'price-changes']:
            return

        self.path.append(name)

        self.current_row.update(self.filedata)

        if attrs:
            self.current_row.update(
                {
                    f"competitor_{aname}": attrs.getValue(aname)
                    for aname
                    in attrs.getNames()
                    if aname in PricesHandler.PRICE_ATTR_NAMES
                }
            )

    def endElement(self, name):
        if name in ['price-check', 'date', 'price-changes']:
            return

        if self.path[-1] == 'product':
            self.current_row = {}

        self.path.pop()

    def characters(self, content):
        if not self.path or not content.strip():
            return

        colname = self.path[-1]
        self.current_row[colname] = content

        if colname == 'price' and self.path[-2] == 'prices':
            self.writer.writerow(self.current_row)
            del self.current_row['price']
            for cname in PricesHandler.PRICE_ATTR_NAMES:
                with suppress(KeyError):
                    del self.current_row[cname]


if __name__ == '__main__':

    utctime_started = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    logging.basicConfig(format='%(name)s, %(asctime)s, %(levelname)s, %(message)s',
                        level=logging.DEBUG)
    kbc_datadir = os.getenv("KBC_DATADIR")
    cfg = docker.Config(kbc_datadir)
    parameters = cfg.get_parameters()

    # log parameters (excluding sensitive designated by '#')
    logging.info({k: v for k, v in parameters.items() if "#" not in k})

    wanted_columns = parameters.get("wanted_columns")
    allowed_file_patterns = parameters.get("allowed_file_patterns")
    forbidden_file_patterns = parameters.get("forbidden_file_patterns")

    # read last_processed timestamp
    with open(f'{kbc_datadir}in/tables/last_processed_timestamp.csv') as input_file:
        last_processed_timestamp = [
            str(ts.replace('"', ''))
            for ts
            # read all input file rows, except the header
            in input_file.read().split(os.linesep)[1:]
        ][0]

    # read list of files we want to download regardless of their timestamp
    # should serve only for backfill and debug
    with open(f'{kbc_datadir}in/tables/input_filelist.csv') as input_file:
        input_fileset = {
            str(name.replace('"', ''))
            for name
            # read all input file rows, except the header
            in input_file.read().split(os.linesep)[1:]
        }

    # connect to s3 bucket
    session = boto3.Session(aws_access_key_id=parameters.get("aws_key"),
                            aws_secret_access_key=parameters.get("#aws_secret"))
    s3 = session.resource("s3")
    esol_bucket = s3.Bucket(parameters.get("bucket_name"))

    # download xml files that are xml, were not present in the last download, have allowed pattern
    # or specifically enumerated files
    files_to_download = (file for file in esol_bucket.objects.all()
                           if (file.key.endswith(".xml")
                           and any(name_pattern in file.key for name_pattern in allowed_file_patterns)
                           and all(name_pattern not in file.key for name_pattern in forbidden_file_patterns)
                           and file.last_modified.replace(tzinfo=None) >
                           datetime.datetime.strptime(last_processed_timestamp, "%Y-%m-%d %H:%M:%S"))
                           or (file.key in input_fileset)
                         )
    # we need to reuse the generator
    # copy it to save memory
    files_to_download, files_to_download_backup = tee(files_to_download)

    max_timestamp_this_run = [{"max_timestamp_this_run": max(file.last_modified
                                                             for file in files_to_download_backup).replace(
        tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")}]

    logging.info("Collected files to download.")

    # create temp directory to store downloaded files
    if not os.path.exists(f'{kbc_datadir}downloaded_xmls'):
        os.makedirs(f'{kbc_datadir}downloaded_xmls')

    logging.info("Downloading files.")

    files_to_process = []

    for file in files_to_download:
        filepath = f"{kbc_datadir}downloaded_xmls/" + file.key.split('/')[-1]
        files_to_process.append(filepath)
        with open(filepath, "wb") as f:
            esol_bucket.download_fileobj(file.key, f)

        logging.info(f"File {filepath.split('/')[-1]} downloaded.")

    del files_to_download, session, s3, esol_bucket

    with open(f"{kbc_datadir}out/tables/esolutions_last_timestamp.csv", 'w+', encoding="utf-8") as f:
        dict_writer = DictWriter(f, ["max_timestamp_this_run"])
        dict_writer.writeheader()
        dict_writer.writerows(max_timestamp_this_run)

    with open(f'{kbc_datadir}out/tables/esolutions_prices.csv', 'w', encoding='utf8') as csvfile:
        dw = DictWriter(csvfile, fieldnames=list(set(wanted_columns + ["utctime_started", "filename"])))
        dw.writeheader()

        for file in files_to_process:

            filename = file.split('/')[-1]
            logging.info(f"Processing file {filename}")

            h = PricesHandler(dw, {'filename': filename, 'utctime_started': utctime_started})
            xml.sax.parse(file, h)
            logging.info(f"File {filename} processing finished.")

    logging.info("Done.")

