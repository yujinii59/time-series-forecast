from common.name import Key
from typing import Dict
import pandas as pd

class Load(object):
    """
    data_vrsn o
    sales_info
    master ( cust, item, calendar, sale_matrix, model, hyperparam)
    외부데이터(날씨)
    """

    def __init__(self, io, sql, data_vrsn, period, common):
        self.io = io
        self.sql = sql
        self.data_vrsn = data_vrsn
        self.period = period
        self.common = common
        self.key = Key()

    def run(self) -> Dict[dict, dict]:
        data = {
            'master': self.load_master(),
            'input': self.load_input()
        }

        return data

    def load_master(self) -> Dict[str, pd.DataFrame]:
        item_mst = self.io.get_df_from_db(sql=self.sql.sql_item_view())
        cust_mst = self.io.get_df_from_db(sql=self.sql.sql_cust_grp_info())
        calendar_mst = self.io.get_df_from_db(sql=self.sql.sql_calendar())
        sales_matrix = self.io.get_df_from_db(sql=self.sql.sql_sales_matrix())
        model_info = self.io.get_df_from_db(sql=self.sql.sql_algorithm(**{'division': 'FCST'}))
        hyper_param = self.io.get_df_from_db(sql=self.sql.sql_best_hyper_param_grid())

        master = {
            self.key.item_mst: item_mst,
            self.key.cust_mst: cust_mst,
            self.key.calendar_mst: calendar_mst,
            self.key.sales_matrix: sales_matrix,
            self.key.model: model_info,
            self.key.hyper_param: hyper_param
        }

        return master

    def load_input(self) -> Dict[str, pd.DataFrame]:
        sell_in = self.io.get_df_from_db(sql=self.sql.sql_sell_in(**self.period))
        exg_data = self.io.get_df_from_db(sql=self.sql.sql_exg_data(**self.period))

        input_data = {
            self.key.sell_in: sell_in,
            self.key.exg_data: exg_data
        }

        return input_data
