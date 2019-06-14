"""Module to perform data cleaning functions on EPA IPM data tables."""

import logging
import pandas as pd
import numpy as np
import pudl
import pudl.constants as pc
from pudl.helpers import simplify_columns

logger = logging.getLogger(__name__)


def load_curves(epaipm_dfs, epaipm_transformed_dfs):
    """
    Pull and transform the load curve table from wide to tidy format.

    Args:
        epaipm_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a table from
            EPA's IPM, as reported in the Excel spreadsheets they distribute.
        epa_ipm_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.

    """
    lc = epaipm_dfs['load_curves_ipm'].copy()
    lc = simplify_columns(lc)
    # Melt the load curves
    melt_lc = lc.melt(
        id_vars=['region', 'month', 'day'],
        var_name='hour',
        value_name='load_mw'
    )

    melt_lc['hour'] = (
        melt_lc['hour'].str.replace('hour_', '').astype(int)
    )
    # IPM hour designations are 1-24. Convert to 0-23 to match datetime.
    melt_lc['hour'] -= 1

    # Group to easily create 8760 time_index
    grouped = melt_lc.groupby('region')

    df_list = []
    for _, df in grouped:
        df = df.sort_values(['month', 'day', 'hour'])
        df = df.reset_index(drop=True)
        df['time_index'] = df.index + 1
        df_list.append(df)

    tidy_load_curves = pd.concat(df_list)

    epaipm_transformed_dfs['load_curves_ipm'] = tidy_load_curves

    return epaipm_transformed_dfs


def transmission_single(epaipm_dfs, epaipm_transformed_dfs):
    """
    Pull and transform the transmission constraints between individual regions
    table, renaming columns.

    Args:
        epaipm_dfs (dictionary of pandas.DataFrame): Each entry in this
            dictionary of DataFrame objects corresponds to a table from
            EPA's IPM, as reported in the Excel spreadsheets they distribute.
        epa_ipm_transformed_dfs (dictionary of DataFrames)

    Returns: transformed dataframe.

    """
    trans_df = epaipm_dfs['transmission_single_ipm'].copy()
    trans_df = trans_df.reset_index()
    trans_df = trans_df.rename(
        columns=pc.epaipm_rename_dict['transmission_single_ipm']
    )
    epaipm_transformed_dfs['transmission_single_ipm'] = trans_df

    return epaipm_transformed_dfs


def transform(epaipm_raw_dfs, epaipm_tables=pc.epaipm_pudl_tables):
    """Transform EPA IPM dfs."""
    epaipm_transform_functions = {
        'transmission_single_ipm': transmission_single,
        'load_curves_ipm': load_curves,
    }
    epaipm_transformed_dfs = {}

    if not epaipm_raw_dfs:
        logger.info("No raw EPA IPM dataframes found. "
                    "Not transforming EPA IPM.")
        return epaipm_transformed_dfs

    for table in epaipm_transform_functions:
        if table in epaipm_tables:
            logger.info(f"Transforming raw EPA IPM DataFrames for {table}")
            epaipm_transform_functions[table](epaipm_raw_dfs,
                                              epaipm_transformed_dfs)

    return epaipm_transformed_dfs