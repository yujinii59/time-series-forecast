import pandas as pd
import common.config as config
from collections import defaultdict

class Process(object):
    def __init__(self, init, data):
        self.init = init
        self.data = data

    def process(self):
        sales_rst = self.reformat_sales_rst(sales_rst=self.data['input'][config.key_sell_in], areas=config.EXG_MAP)
        exg_data = self.split_exg_data(data=self.data['input'][config.key_exg_data])
        merged = self.merge_data(exg=exg_data, sales_rst=sales_rst)
        hrchy_data = self.set_item_hrchy(data=merged, apply=self.init.hrchy['apply'], lvl=self.init.hrchy['recur_lvl']['total'])

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
        return merged

    def set_item_hrchy(self, data: pd.DataFrame, apply: list, lvl: int):
        unique_data = data[apply].drop_duplicates().reset_index()
        length = len(unique_data)
        unique_data = unique_data.to_dict()

        hrchy = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(pd.DataFrame))))
        for i in range(length):
            target = hrchy
            df = data
            for j, cd in enumerate(apply):
                key = unique_data[cd][i]
                if j == lvl -1:
                    target[key] = df[df[cd]==key]
                else:
                    target = target[key]
                    df = df[df[cd] == key]

        return hrchy


