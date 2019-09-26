"""Retrieve data from NREL's Annual Technology Baseline (ATB) Data."""

import logging
from pathlib import Path

import pandas as pd
import pudl.constants as pc
import pudl.workspace.datastore as datastore

logger = logging.getLogger(__name__)


def get_nrelatb_name(file, year, data_dir):
    """Returns the appropriate NREL ATB csv file.

    Args:
        file (str): The file that we're trying to read data for.
        year (int): The year that we're trying to read data for.
        data_dir (path-like): Path to the top directory of the PUDL datastore.

    Returns:
        str: The path to NREL ATB spreadsheet.

    """
    # Access the CSV distributed with PUDL:
    nrelatb_dir = Path(datastore.path(
        source='nrelatb', data_dir=data_dir, year=year, file=False))

    # FIXME this appears to be needed if reading tabs
    # pattern = pc.files_dict_nrelatb[file]
    # name = sorted(epaipm_dir.glob(pattern))[0]

    return Path(nrelatb_dir, file)


def get_nrelatb_file(filename, year, read_file_args, data_dir):
    """Reads in files to create dataframes.

    No need to use ExcelFile objects with the ATB files because each file
    is only a single sheet.

    Args:
        filename (str): ['2019-ATB-Market-20.csv', 2019-ATB-RD-20.csv',
            '2019-ATB-Market-30.csv', '2019-ATB-RD-30.csv', '2019-ATB-RD-Life.csv']
        year (int): The year that we're trying to read data for.
        read_file_args (dict): dictionary of arguments for pandas read_*
        data_dir (path-like): Path to the top directory of the PUDL datastore.

    Returns:
        pandas.io.excel.ExcelFile: an xlsx file of NREL ATB data

    """
    nrelatb_file = {}
    logger.info(
        f"Extracting data from NREL ATB {filename} spreadsheet.")

    full_filename = get_nrelatb_name(
        file=filename,
        year=year,
        data_dir=data_dir
    )

    nrelatb_file = pd.read_csv(
        full_filename,
        **read_file_args
    )

    return nrelatb_file


def create_dfs_nrelatb(files, nrelatb_years, data_dir):
    """Makes dictionary of pages (keys) to dataframes (values) for nrelatb tabs.

    Args:
        files (list): a dictionary of nrelatb files
        nrelatb_years (list): a list of years
        data_dir (path-like): Path to the top directory of the PUDL datastore.
    Returns:
        dict: dictionary of pages (key) to dataframes (values)

    """
    # Prep for ingesting nrelatb
    # Create excel objects
    nrelatb_dfs = {}

    # The 2019 NREL ATB csv files are in a tidy format and do not
    # appear to need extra flags when running read_csv
    read_file_args = {}

    for year in nrelatb_years:
        for pudl_name, filename in files.items():
            nrelatb_dfs[pudl_name] = get_nrelatb_file(
                filename,
                year,
                read_file_args,
                data_dir
            )

    return nrelatb_dfs


def extract(nrelatb_years, data_dir):
    """Extracts data from ATB files.

    Args:
        nrelatb_years (list): a list of data_years
        data_dir (path-like): Path to the top directory of the PUDL datastore.

    Returns:
        dict: dictionary of DataFrames with extracted (but not yet transformed)
        data from each file.

    """
    # Prep for ingesting NREL ATB
    # create raw atb dfs from spreadsheets

    logger.info('Beginning ETL for NREL ATB.')

    nrelatb_raw_dfs = create_dfs_nrelatb(
        files=pc.nrelatb_files,
        nrelatb_years=nrelatb_years,
        data_dir=data_dir)
    return nrelatb_raw_dfs
