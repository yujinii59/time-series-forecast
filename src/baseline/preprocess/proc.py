import pandas as pd
import common.config as config

class Process(object):
    def __init__(self, init, data):
        self.init = init
        self.data = data

    def process(self):
        sales_rst = self.area_mapping(sales_rst=self.data['input'][config.key_sell_in], areas=config.EXG_MAP)
        self.merge_data(exg=self.data['input'][config.key_exg_data], sales_rst=sales_rst)

    def area_mapping(self, sales_rst: pd.DataFrame, areas: dict) -> pd.DataFrame:
        sales_rst['idx_dtl_cd'] = [areas[cust] for cust in sales_rst['cust_grp_cd']]

        return sales_rst

    def merge_data(self, exg: pd.DataFrame, sales_rst: pd.DataFrame):
        merged = pd.merge(exg, sales_rst, how='inner', on=['yymmdd', 'idx_dtl_cd'])

    def set_item_hrchy(self, data: pd.DataFrame, lvl: list):
        pass

