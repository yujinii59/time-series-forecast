import pandas as pd
import common.config as config


class Process(object):
    def __init__(self, init, data):
        self.init = init
        self.data = data
        self.common = init.common
        self.hrchy_list = self.init.hrchy['apply']
        self.hrchy_lvl = self.init.hrchy['recur_lvl']['total'] - 1

    def process(self):
        sales_rst = self.reformat_sales_rst(sales_rst=self.data['input'][config.key_sell_in], areas=config.EXG_MAP)
        exg_data = self.split_exg_data(data=self.data['input'][config.key_exg_data])
        merged = self.merge_data(exg=exg_data, sales_rst=sales_rst)
        hrchy_data = self.set_item_hrchy(data=merged, hrchy_list=self.hrchy_list, lvl=0, hrchy_lvl=self.hrchy_lvl)
        filtered_data = self.filtering_data_total_week(data=hrchy_data, target='sku_cd',
                                                       threshold=self.common['filter_threshold_cnt'],
                                                       lvl=0, hrchy_lvl=self.hrchy_lvl)

        resampled_data = self.resampling_data(data=hrchy_data, hrchy_list=self.hrchy_list, lvl=0, hrchy_lvl=self.hrchy_lvl)

    def reformat_sales_rst(self, sales_rst: pd.DataFrame, areas: dict) -> pd.DataFrame:
        sales_rst['idx_dtl_cd'] = [areas[cust] for cust in sales_rst['cust_grp_cd']]
        sales_rst = sales_rst.drop(columns=['division_cd', 'seq', 'from_dc_cd', 'unit_price', 'unit_cd', 'create_date'])

        return sales_rst

    def split_exg_data(self, data: pd.DataFrame) -> pd.DataFrame:
        idx_cd = data['idx_cd'].unique()
        split_data = pd.DataFrame()
        for idx in idx_cd:
            if len(split_data) == 0:
                split_data = data[data['idx_cd'] == idx]
                split_data = split_data.rename(columns={'ref_val': idx.lower()})
                split_data = split_data.drop(columns='idx_cd')
            else:
                split_data_tmp = data[data['idx_cd'] == idx]
                split_data_tmp = split_data_tmp.rename(columns={'ref_val': idx.lower()})
                split_data_tmp = split_data_tmp.drop(columns='idx_cd')
                split_data = pd.merge(split_data, split_data_tmp, how='inner', on=['idx_dtl_cd', 'yymmdd'])

        return split_data

    def merge_data(self, exg: pd.DataFrame, sales_rst: pd.DataFrame) -> pd.DataFrame:
        merged = pd.merge(sales_rst, exg, how='inner', on=['yymmdd', 'idx_dtl_cd'])
        del merged['idx_dtl_cd']

        return merged

    def set_item_hrchy(self, data: pd.DataFrame, hrchy_list: list, lvl: int, hrchy_lvl: int):
        unique_data = data[hrchy_list[lvl]].unique()

        hrchy = {}
        for uniq in unique_data:
            if lvl < hrchy_lvl:
                hrchy[uniq] = self.set_item_hrchy(data=data[data[hrchy_list[lvl]] == uniq], hrchy_list=hrchy_list,
                                                  lvl=lvl + 1, hrchy_lvl=hrchy_lvl)

            else:
                hrchy[uniq] = data[data[hrchy_list[lvl]] == uniq]

        return hrchy

    def resampling_data(self, data: dict, hrchy_list: list, lvl: int, hrchy_lvl: int):
        group_by_data = {}
        cols = hrchy_list + ['yymmdd', 'week', 'gsr_sum', 'rhm_avg', 'discount', 'qty']
        group_cols = hrchy_list + ['yymmdd', 'week', 'gsr_sum', 'rhm_avg']
        for key, val in data.items():
            if lvl < hrchy_lvl:
                group_by_data[key] = self.resampling_data(data=val, hrchy_list=hrchy_list, lvl=lvl + 1, hrchy_lvl=hrchy_lvl)
            else:
                tmp: pd.DataFrame = val.copy()
                group_by_data[key] = tmp[cols].groupby(by=group_cols).agg({'discount': 'mean', 'qty': 'sum'})
                print("")

        return group_by_data

    def filtering_data_total_week(self, data: dict, target: str, threshold: str, lvl: int, hrchy_lvl: int):
        filtered_data = {}
        for key, val in data.items():
            if lvl < hrchy_lvl:
                filtered_data[key] = self.filtering_data_total_week(data=val, target=target, threshold=threshold,
                                                                    lvl=lvl+1, hrchy_lvl=hrchy_lvl)
            else:
                threshold = int(threshold)
                data_tmp: pd.DataFrame = val.copy()
                tmp = data_tmp[[target, 'week']].drop_duplicates()
                tmp = tmp.groupby(by=target).count().reset_index()
                tmp = tmp.rename(columns={'week': 'cnt'})
                tmp = tmp[tmp['cnt'] > threshold]
                filtered = list(tmp[target])

                data_tmp = data_tmp[data_tmp[target].isin(filtered)]
                filtered_data[key] = data_tmp
                print("")

        return filtered_data



    def filtering_data_recent_week(self, data: dict, threshold: int, last_day: str, target: str):
        pass
