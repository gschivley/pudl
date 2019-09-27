"""Module to perform data cleaning functions on NREL ATB data tables."""

import logging

import pandas as pd
import pudl.constants as pc
from pudl.helpers import simplify_columns

logger = logging.getLogger(__name__)

ATB_COL_RENAME = {
    'CRPYears': 'cap_recovery_years',
    'CostCase': 'cost_case',
    'FinancialCase': 'financial_case',
    'Key': 'key',
    'Pivot Field Names': 'basis_year',
    'Pivot Field Values': 'parameter_value',
    'Table Name': 'parameter',
    'TechDetail': 'tech_detail',
    'Technology': 'technology',
}


def transform(nrelatb_raw_dfs, nrelatb_tables=pc.nrelatb_pudl_tables):
    """
    Transform NREL ATB DataFrames.

    Args:
        nrelatb_raw_dfs(dict): a dictionary of table names(keys) and
            DataFrames(values)
        nrelatb_tables(list): The list of NREL ATB tables that can be
            successfully pulled into PUDL

    Returns:
        dict: A dictionary of DataFrame objects in which tables from NREL
        ATB(keys) correspond to normalized DataFrames of values from
        that table(values)
    """
    table_list = []

    for tablename, table in nrelatb_raw_dfs.items():
        logger.info(f"Transforming raw NREL ATB DataFrames for {tablename}")
        src, mkt, financial = tablename.split("_")
        table = table.rename(columns=ATB_COL_RENAME)
        table = table.reindex(columns=ATB_COL_RENAME.values())
        # table["maturity"] = mkt
        # table["cap_recovery_period"] = financial

        table_list.append(table)

    nrelatb_transformed_df = pd.concat(table_list, ignore_index=True)
    nrelatb_transformed_df['parameter'] = (
        nrelatb_transformed_df['parameter'].str.replace('Summary_', '')
    )

    # Need to pivot and change the "parameter" column values into column headers,
    # then clean up the names and get rid of the column multi-index
    index_cols = [
        'key',
        'technology',
        'cap_recovery_years',
        'cost_case',
        'financial_case',
        'basis_year',
        'tech_detail',
        'parameter',
    ]
    # There are some duplicate rows that can be dropped.
    nrelatb_transformed_df = nrelatb_transformed_df.drop_duplicates()
    nrelatb_transformed_df.set_index(index_cols, inplace=True)
    nrelatb_transformed_df = nrelatb_transformed_df.unstack(level=-1)
    nrelatb_transformed_df.columns = nrelatb_transformed_df.columns.droplevel(
        0)
    nrelatb_transformed_df = simplify_columns(
        nrelatb_transformed_df.reset_index())

    nrelatb_transformed_df['cap_recovery_years'] = (
        nrelatb_transformed_df['cap_recovery_years'].astype(str)
    )

    return {'technology_costs_nrelatb': nrelatb_transformed_df}
