import pandas as pd
import common.config as config
from typing import Any


class Process(object):
    def __init__(self, init, data):
        self.init = init
        self.data = data

    def process(self):
        sales_rst = self.reformat_sales_rst(sales_rst=self.data['input'][config.key_sell_in], areas=config.EXG_MAP)
        exg_data = self.split_exg_data(data=self.data['input'][config.key_exg_data])
        merged = self.merge_data(exg=exg_data, sales_rst=sales_rst)
        hrchy_data = self.set_item_hrchy(data=merged, hrchy_list=self.init.hrchy['apply'],
                                         lvl=0, max_lvl=self.init.hrchy['recur_lvl']['total'] - 1)
        resample_data = self.resampling_data(data=hrchy_data, hrchy_list=self.init.hrchy['apply'],
                                           lvl=0, max_lvl=self.init.hrchy['recur_lvl']['total'] - 1)

    def reformat_sales_rst(self, sales_rst: pd.DataFrame, areas: dict) -> pd.DataFrame:
        sales_rst['idx_dtl_cd'] = [areas[cust] for cust in sales_rst['cust_grp_cd']]
        del sales_rst['division_cd']
        del sales_rst['seq']
        del sales_rst['from_dc_cd']
        del sales_rst['unit_price']
        del sales_rst['unit_cd']
        del sales_rst['create_date']

        return sales_rst

    def split_exg_data(self, data: pd.DataFrame) -> pd.DataFrame:
        idx_cd = data['idx_cd'].unique()
        split_data = pd.DataFrame()
        for idx in idx_cd:
            if len(split_data) == 0:
                split_data = data[data['idx_cd'] == idx]
                split_data.rename(columns={'ref_val': idx.lower()}, inplace=True)
                del split_data['idx_cd']
            else:
                split_data_tmp = data[data['idx_cd'] == idx]
                split_data_tmp.rename(columns={'ref_val': idx.lower()}, inplace=True)
                del split_data_tmp['idx_cd']
                split_data = pd.merge(split_data, split_data_tmp, how='inner', on=['idx_dtl_cd', 'yymmdd'])

        return split_data

    def merge_data(self, exg: pd.DataFrame, sales_rst: pd.DataFrame) -> pd.DataFrame:
        merged = pd.merge(sales_rst, exg, how='inner', on=['yymmdd', 'idx_dtl_cd'])
        del merged['idx_dtl_cd']

        return merged

    def set_item_hrchy(self, data: pd.DataFrame, hrchy_list: list, lvl: int, max_lvl: int):
        unique_data = data[hrchy_list[lvl]].unique()

        hrchy = {}
        for uniq in unique_data:
            if lvl < max_lvl:
                hrchy[uniq] = self.set_item_hrchy(data=data[data[hrchy_list[lvl]] == uniq], hrchy_list=hrchy_list,
                                                  lvl=lvl + 1, max_lvl=max_lvl)

            else:
                hrchy[uniq] = data[data[hrchy_list[lvl]] == uniq]

        return hrchy

    def resampling_data(self, data: Any, hrchy_list: list, lvl: int, max_lvl: int):
        group_by_data = {}
        cols = hrchy_list + ['yymmdd', 'week', 'gsr_sum', 'rhm_avg', 'discount', 'qty']
        group_cols = hrchy_list + ['yymmdd', 'week', 'gsr_sum', 'rhm_avg']
        for key, val in data.items():
            if lvl < max_lvl:
                group_by_data[key] = self.resampling_data(data=val, hrchy_list=hrchy_list, lvl=lvl + 1, max_lvl=max_lvl)
            else:
                tmp: pd.DataFrame = data[key]
                group_by_data[key] = tmp[cols].groupby(by=group_cols).agg({'discount': 'mean', 'qty': 'sum'})
                print("")

        return group_by_data
