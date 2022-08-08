import common.config as config
import common.util as util

from typing import Tuple
import pandas as pd
import datetime as dt
import numpy as np
from copy import deepcopy
from scipy.stats import mode
from sklearn.impute import KNNImputer


# filtering - threshold 값 기준으로 [몇주차 이하 제외/ 최근 n주차 데이터 없으면 제외(브랜드/SKU)]
# trimming - 데이터 [0,0,0,0,1,2,5,3,6,7] -> [1,2,5,3,6,7]
# Outiler - Sigma / Percentage
# Imputation - 중간중간의 누락 주차 데이터 넣어주는 방법 (median / mod / mean / knn imputation)


class Process(object):
    def __init__(self, init, data):
        self.init = init
        self.data = data
        self.common = init.common
        self.hrchy_lvl = self.init.hrchy['recur_lvl']['total'] - 1
        self.item_hrchy_list = self.common['hrchy_item'].split(',')
        self.hrchy_list = self.init.hrchy['apply']
        self.avg_col = self.common['agg_avg'].split(',')
        self.sum_col = self.common['agg_sum'].split(',')
        self.filter_threshold_cnt = int(self.common['filter_threshold_cnt'])
        self.threshold_recent = ''
        self.threshold_sku_recent = ''
        self.outlier_method = 'sigma'
        self.sigma = int(self.common['outlier_sigma'])
        self.percentage = 0.25
        self.imputer = 'mod'

    def process(self):
        sales_rst = self.reformat_sales_rst(sales_rst=self.data['input'][config.key_sell_in], areas=config.EXG_MAP)

        exg_data = self.split_exg_data(data=self.data['input'][config.key_exg_data])

        grouped = self.grouping_data_by_week(data=sales_rst)

        merged = self.merge_data(exg=exg_data, sales_rst=grouped,
                                 calendar_data=self.data['master'][config.key_calendar_mst])

        hrchy_data = self.set_item_hrchy(data=merged, hrchy_list=self.hrchy_list, lvl=0, hrchy_lvl=self.hrchy_lvl)

        self.set_threshold_yymmdd(common=self.common, calendar_data=self.data['master'][config.key_calendar_mst],
                                  last_day=self.init.period['to'])

        preprocessed_data = util.drop_down_hrchy_data(data=hrchy_data, fn=self.preprocessing_data, lvl=0,
                                                      hrchy_lvl=self.hrchy_lvl)

        return preprocessed_data

    def set_threshold_yymmdd(self, common: dict, calendar_data: pd.DataFrame, last_day: str):
        last_day = last_day
        last_day = dt.datetime.strptime(last_day, '%Y%m%d')
        threshold_yymmdd = last_day - dt.timedelta(weeks=int(common['filter_threshold_recent']))
        threshold_sku_yymmdd = last_day - dt.timedelta(weeks=int(common['filter_threshold_sku_recent']))
        threshold_yymmdd = calendar_data[calendar_data['yymmdd'] == dt.datetime.strftime(threshold_yymmdd, '%Y%m%d')][
            'start_week_day'].values
        threshold_sku_yymmdd = \
            calendar_data[calendar_data['yymmdd'] == dt.datetime.strftime(threshold_sku_yymmdd, '%Y%m%d')][
                'start_week_day'].values

        self.threshold_recent = threshold_yymmdd[0]
        self.threshold_sku_recent = threshold_sku_yymmdd[0]

    @staticmethod
    def reformat_sales_rst(sales_rst: pd.DataFrame, areas: dict) -> pd.DataFrame:
        reformat_data = deepcopy(sales_rst)
        reformat_data['idx_dtl_cd'] = [areas[cust] for cust in reformat_data['cust_grp_cd']]
        reformat_data = reformat_data.drop(
            columns=['division_cd', 'seq', 'from_dc_cd', 'unit_price', 'unit_cd', 'create_date'])

        return reformat_data

    @staticmethod
    def split_exg_data(data: pd.DataFrame) -> pd.DataFrame:
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

    @staticmethod
    def merge_data(exg: pd.DataFrame, sales_rst: pd.DataFrame, calendar_data: pd.DataFrame) -> pd.DataFrame:
        merged = pd.merge(sales_rst, exg, how='inner', on=['yymmdd', 'idx_dtl_cd'])
        merged = merged.drop(columns=['idx_dtl_cd'])
        merged = pd.merge(merged, calendar_data[['yymmdd', 'start_week_day']], how='inner', on='yymmdd')
        merged = merged.drop(columns=['yymmdd'])
        merged = merged.rename(columns={'start_week_day': 'yymmdd'})

        return merged

    @staticmethod
    def needed_columns(columns: list, avg_col: list, sum_col: list) -> Tuple[dict, list]:
        agg_dic = {}
        agg_col = []
        for avg in avg_col:
            if avg in columns:
                agg_dic[avg] = 'mean'
                agg_col.append(avg)
        for summa in sum_col:
            if summa in columns:
                agg_dic[summa] = 'sum'
                agg_col.append(summa)

        groupby_col = [col for col in columns if col not in agg_col]

        return agg_dic, groupby_col

    def grouping_data_by_week(self, data: pd.DataFrame) -> pd.DataFrame:
        avg_col, sum_col = self.avg_col, self.sum_col
        columns = data.columns.tolist()
        agg_dic, groupby_col = self.needed_columns(columns=columns, avg_col=avg_col, sum_col=sum_col)
        groupby_col.remove('yymmdd')
        agg_dic['yymmdd'] = 'min'
        grouping_data = deepcopy(data)
        grouping_data = grouping_data.groupby(by=groupby_col).agg(agg_dic)
        grouping_data = grouping_data.reset_index()

        return grouping_data

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

    def preprocessing_data(self, data: pd.DataFrame):
        data_tmp = deepcopy(data)

        filtered_data = self.filtering_data(data=data_tmp, hrchy_list=self.item_hrchy_list)

        resampled_data = self.resampling_data(data=filtered_data)

        filtered_data = self.filtering_data(data=resampled_data, hrchy_list=self.hrchy_list)

        trimmed_data = self.trimming_data(data=filtered_data)

        cleansed_data = self.outlier_setting(data=trimmed_data)

        imputed_data = self.imputation_data(data=cleansed_data)

        return imputed_data

    def resampling_data(self, data: pd.DataFrame) -> pd.DataFrame:
        avg_col, sum_col = self.avg_col, self.sum_col
        tmp = data.copy()
        item_hrchy_list = self.item_hrchy_list
        hrchy_list = self.hrchy_list
        drop_col = []
        for hrchy in item_hrchy_list:
            if hrchy not in hrchy_list:
                drop_col.append(hrchy)

        tmp = tmp.drop(columns=drop_col)
        columns = tmp.columns.tolist()
        agg_dic, groupby_col = self.needed_columns(columns=columns, avg_col=avg_col, sum_col=sum_col)
        resample_data = tmp.groupby(by=groupby_col).agg(agg_dic).reset_index()

        return resample_data

    def filtering_data(self, data: pd.DataFrame, hrchy_list: list):
        data_tmp = deepcopy(data)
        filtered_data = self.filtering_data_total_week(data=data_tmp, hrchy_list=hrchy_list)
        filtered_data = self.filtering_data_recent_week(data=filtered_data, hrchy_list=hrchy_list)

        return filtered_data

    def filtering_data_total_week(self, data: pd.DataFrame, hrchy_list: list) -> pd.DataFrame:
        target = hrchy_list[-1]
        threshold = self.filter_threshold_cnt
        filtered_data = data.copy()
        tmp = filtered_data[[target, 'yymmdd']].groupby(by=target).count().reset_index()
        tmp = tmp.rename(columns={'yymmdd': 'cnt'})
        tmp = tmp[tmp['cnt'] > threshold]
        filtered = list(tmp[target])

        filtered_data = filtered_data[filtered_data[target].isin(filtered)]

        return filtered_data

    def filtering_data_recent_week(self, data: pd.DataFrame, hrchy_list: list) -> pd.DataFrame:
        filtered_data = data.copy()
        target = hrchy_list[-1]

        if target == 'sku_cd':
            threshold = self.threshold_sku_recent

        else:
            threshold = self.threshold_recent

        filtered_data = filtered_data[filtered_data['yymmdd'] < threshold]
        tmp = filtered_data[[target, 'yymmdd']].groupby(by=target).count().reset_index()
        tmp = tmp.rename(columns={'yymmdd': 'cnt'})
        tmp = tmp[tmp['cnt'] > 0]
        filtered = list(tmp[target])

        filtered_data = filtered_data[filtered_data[target].isin(filtered)]

        return filtered_data

    @staticmethod
    def trimming_data(data: pd.DataFrame) -> pd.DataFrame:
        trimmed_data = data.copy()
        trimmed_data = trimmed_data.iloc[np.trim_zeros(filt=trimmed_data['qty'], trim='f').index.tolist()]

        return trimmed_data

    def outlier_setting(self, data: pd.DataFrame) -> pd.DataFrame:
        data_qty_tmp = deepcopy(data['qty'])
        qty = data_qty_tmp.values
        mean = np.mean(qty)
        lower = data_qty_tmp.quantile(0)
        upper = data_qty_tmp.quantile(1)

        if self.outlier_method == 'sigma':
            std = np.std(qty)
            sigma = self.sigma * std
            lower = mean - sigma
            upper = mean + sigma

        elif self.outlier_method == 'iqr':
            q1 = data_qty_tmp.quantile(0.25)
            q3 = data_qty_tmp.quantile(0.75)
            iqr = q3 - q1
            lower = mean - 1.5 * iqr
            upper = mean + 1.5 * iqr

        elif self.outlier_method == 'percentage':
            lower = data_qty_tmp.quantile(self.percentage)
            upper = data_qty_tmp.quantile(1 - self.percentage)

        qty = np.where(qty < lower, lower, qty)  # np.where('조건', true, false)
        qty = np.where(qty > upper, upper, qty)

        data['qty'] = qty

        return data

    def imputation_data(self, data: pd.DataFrame) -> pd.DataFrame:
        data_qty_tmp = deepcopy(data['qty'])
        qty = data_qty_tmp.values
        if self.imputer == 'before':
            for i in range(1, len(qty)):
                if qty[i] == 0:
                    qty[i] = qty[i - 1]

        elif self.imputer == 'avg':
            for i in range(1, len(qty) - 1):
                if qty[i] == 0:
                    qty[i] = (qty[i - 1] + qty[i + 1]) / 2

        elif self.imputer == 'median':
            med = np.median(qty)
            qty = np.where(qty == 0, med, qty)

        elif self.imputer == 'mod':
            mod = round(np.median(mode(qty).mode), 3)
            qty = np.where(qty == 0, mod, qty)

        elif self.imputer == 'mean':
            mean = np.mean(qty)
            qty = np.where(qty == 0, mean, qty)

        elif self.imputer == 'knn':
            if qty:
                qty = np.where(qty == 0, np.nan, qty)
                qty = qty.reshape(-1, 1)
                imputer = KNNImputer(n_neighbors=3)
                qty = imputer.fit_transform(qty)
                qty = qty.ravel()

        data['qty'] = qty

        return data
