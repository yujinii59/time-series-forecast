import common.config as config
import common.util as util
from feature_engineering.selection import Selection

from typing import Tuple
import pandas as pd
import datetime as dt
import numpy as np
from copy import deepcopy
from scipy.stats import mode
from sklearn.impute import KNNImputer


class Process(object):
    def __init__(self, init, data):
        self.init = init
        self.start_day = self.init.period['from']
        self.end_day = self.init.period['to']
        self.data = data
        self.calendar_data = self.data['master'][config.key_calendar_mst]
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
        self.exg_list = []
        self.exg_data = pd.DataFrame()

    def process(self):
        self.calendar_data = self.set_calendar(start_day=self.start_day, end_day=self.end_day)

        sales_rst = self.reformat_sales_rst(sales_rst=self.data['input'][config.key_sell_in])

        sales_rst = self.location_mapping(sales_rst=sales_rst, areas=config.EXG_MAP)

        exg_data, self.exg_list = self.split_exg_data(data=self.data['input'][config.key_exg_data])

        self.exg_data = self.merge_cal_data(data=exg_data, calendar_data=self.calendar_data)

        self.exg_data = self.grouping_data_by_week(data=self.exg_data)

        merged = self.merge_exg_data(data=sales_rst, exg=exg_data)

        merged = self.merge_cal_data(data=merged, calendar_data=self.calendar_data)

        sales_rst = self.grouping_data_by_week(data=merged)

        selection = Selection(exg_list=self.exg_list)
        feature_selected, self.exg_list = selection.selection(data=sales_rst)

        hrchy_data = self.set_item_hrchy(data=feature_selected, hrchy_list=self.hrchy_list, lvl=0,
                                         hrchy_lvl=self.hrchy_lvl)

        self.set_threshold_yymmdd(common=self.common, calendar_data=self.calendar_data,
                                  last_day=self.end_day)

        preprocessed_data, hrchy_cnt = util.drop_down_hrchy_data(data=hrchy_data, fn=self.preprocessing_data, lvl=0,
                                                                 hrchy_lvl=self.hrchy_lvl)

        return preprocessed_data, self.exg_list, hrchy_cnt

    def set_calendar(self, start_day, end_day):
        calendar_data = self.calendar_data
        calendar_data = calendar_data[calendar_data['yymmdd'] >= start_day]
        calendar_data = calendar_data[calendar_data['yymmdd'] <= end_day]

        return calendar_data

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
    def reformat_sales_rst(sales_rst: pd.DataFrame) -> pd.DataFrame:
        reformat_data = deepcopy(sales_rst)
        reformat_data = reformat_data.drop(
            columns=['division_cd', 'seq', 'from_dc_cd', 'unit_price', 'unit_cd', 'create_date'])

        return reformat_data

    @staticmethod
    def location_mapping(sales_rst: pd.DataFrame, areas: dict) -> pd.DataFrame:
        mapping_data = deepcopy(sales_rst)
        mapping_data['idx_dtl_cd'] = [areas[cust] for cust in mapping_data['cust_grp_cd']]

        return mapping_data

    def split_exg_data(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
        idx_cd = data['idx_cd'].unique()
        exg_list = []
        split_data = pd.DataFrame()
        for idx in idx_cd:
            exg_list.append(idx.lower())
            if len(split_data) == 0:
                split_data = data[data['idx_cd'] == idx]
                split_data = split_data.rename(columns={'ref_val': idx.lower()})
                split_data = split_data.drop(columns='idx_cd')
            else:
                split_data_tmp = data[data['idx_cd'] == idx]
                split_data_tmp = split_data_tmp.rename(columns={'ref_val': idx.lower()})
                split_data_tmp = split_data_tmp.drop(columns='idx_cd')
                split_data = pd.merge(split_data, split_data_tmp, how='inner', on=['idx_dtl_cd', 'yymmdd'])

        return split_data, exg_list

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

        grouping_data = deepcopy(data)
        grouping_data = grouping_data.groupby(by=groupby_col).agg(agg_dic)
        grouping_data = grouping_data.reset_index()

        return grouping_data

    @staticmethod
    def merge_exg_data(data: pd.DataFrame, exg: pd.DataFrame) -> pd.DataFrame:
        merged = pd.merge(data, exg, how='inner', on=['yymmdd', 'idx_dtl_cd'])
        merged = merged.drop(columns=['idx_dtl_cd'])

        return merged

    @staticmethod
    def merge_cal_data(data: pd.DataFrame, calendar_data: pd.DataFrame) -> pd.DataFrame:
        if 'week' in data.columns:
            merged = pd.merge(data, calendar_data[['yymmdd', 'start_week_day']], how='inner', on='yymmdd')
        else:
            merged = pd.merge(data, calendar_data[['yymmdd', 'week', 'start_week_day']], how='inner', on='yymmdd')

        merged = merged.drop(columns=['yymmdd'])
        merged = merged.rename(columns={'start_week_day': 'yymmdd'})

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

    def preprocessing_data(self, data: pd.DataFrame):
        data_tmp = deepcopy(data)

        filtered_data = self.filtering_data(data=data_tmp, hrchy_list=self.item_hrchy_list)

        if not len(filtered_data):
            return filtered_data

        resampled_data = self.resampling_data(data=filtered_data)

        filtered_data = self.filtering_data(data=resampled_data, hrchy_list=self.hrchy_list)

        if not len(filtered_data):
            return filtered_data

        fill_zero_data = self.fill_zero(data=filtered_data)

        trimmed_data = self.trimming_data(data=fill_zero_data)

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

    def fill_zero(self, data: pd.DataFrame):
        fill_zero_data = deepcopy(data)
        fill_zero_data = fill_zero_data.drop(columns=self.exg_list)
        fill_data = fill_zero_data.loc[0]
        idx_dtl_cd = config.EXG_MAP[fill_data['cust_grp_cd']]
        exg_data = self.exg_data[self.exg_data['idx_dtl_cd'] == idx_dtl_cd]
        exg_data = exg_data.drop(columns='idx_dtl_cd')
        exg_data = exg_data.sort_values(by='yymmdd')
        for hrchy in self.hrchy_list:
            exg_data[hrchy] = fill_data[hrchy]

        fill_zero_data = pd.merge(fill_zero_data, exg_data, how='right', on=['yymmdd','week'] + self.hrchy_list)
        fill_zero_data = fill_zero_data.fillna(0)

        return fill_zero_data

    def fill_zero_tmp(self, data: pd.DataFrame):
        fill_zero_data = deepcopy(data)
        yymmdd = data['yymmdd']
        fill_data = data.loc[0]
        cal = self.calendar_data[['start_week_day', 'week']].drop_duplicates().reset_index(drop=True)
        l = len(cal)
        for i in range(l):
            date = cal['start_week_day'][i]
            week = cal['week'][i]
            if date not in yymmdd.values:
                fill_data['yymmdd'] = date
                fill_data['week'] = week
                fill_data['qty'] = 0
                fill_data['discount'] = 0
                fill_zero_data = fill_zero_data.append(fill_data, ignore_index=True)

        return fill_zero_data

    @staticmethod
    def trimming_data(data: pd.DataFrame) -> pd.DataFrame:
        trimmed_data = deepcopy(data)
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
