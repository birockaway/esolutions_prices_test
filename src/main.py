from keboola import docker
import xmltodict
from itertools import chain, tee
import logging
import datetime
import csv
import os
import boto3



def parse_item(product_card):
    common_keys = {"product_" + k: v for k, v in product_card.items() if k != "prices"}

    if product_card.get("prices") is not None:
        offer_details = product_card["prices"]["price"]
    else:
        return [common_keys]
    if type(offer_details) != list:
        offer_details = [offer_details]

    eshop_offers = ({**common_keys, **offer_detail} for offer_detail in offer_details)
    return eshop_offers


def process_xml(path, filename, utctime_started):
    with open(path) as fd:
        try:
            doc = xmltodict.parse(fd.read())
            logging.info(f"File {path.split('/')[-1]} converted to dictionary.")
        except Exception as e:
            logging.debug(f"Failed to convert to dict. Filename: {filename}. Exception {e}")
            return None
    try:
        if "-hf-" in path:
            for item in doc["price-changes"]["prices"]["product"]:
                pitem = parse_item(item)
                for row in pitem:
                    yield {
                        **{colname: colval for colname, colval in row.items() if colname in wanted_columns},
                        **{'utctime_started': utctime_started},
                        **{'filename': filename}
                    }
        else:
            for item in doc['price-check']['product']:
                pitem = parse_item(item)
                for row in pitem:
                    yield {
                        **{colname: colval for colname, colval in row.items() if colname in wanted_columns},
                        **{'utctime_started': utctime_started},
                        **{'filename': filename}
                    }


    except Exception as e:
        logging.debug(f"Failed to unnest. Filename: {filename}. Exception {e}")


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
    os.makedirs(f"{kbc_datadir}downloaded_xmls")

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
        dict_writer = csv.DictWriter(f, ["max_timestamp_this_run"])
        dict_writer.writeheader()
        dict_writer.writerows(max_timestamp_this_run)

    for file_order, file_path in enumerate(files_to_process):

        filename = file_path.split('/')[-1]
        logging.info(f"Processing file {filename}")

        with open(f"{kbc_datadir}out/tables/esolutions_prices.csv", 'a+', encoding="utf-8") as f:
            dict_writer = csv.DictWriter(f, wanted_columns + ["utctime_started", "filename"])
            if file_order == 0:
                dict_writer.writeheader()

            for csv_row in process_xml(file_path, filename, utctime_started):
                dict_writer.writerow(csv_row)

        logging.info(f"File {filename} processing finished.")

    logging.info("Done.")

