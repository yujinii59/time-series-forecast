import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from copy import deepcopy


class Selection(object):
    def __init__(self, exg_list):
        self.exg_list = exg_list
        self.target_col = 'qty'
        self.corr_method = 'pearson'
        self.select_col_num = 2

    def selection(self, data: pd.DataFrame):
        data_tmp = deepcopy(data)
        exg_corr = []
        for exg in self.exg_list:
            corr = 0
            if self.corr_method == 'pearson':
                corr = np.corrcoef(data[exg], data[self.target_col])[0][1]
            elif self.corr_method == 'spearman':
                corr = spearmanr(data[exg], data[self.target_col]).correlation
            exg_corr.append((abs(corr), exg))

        exg_corr = sorted(exg_corr, reverse=True)
        exg_list = [exg for corr, exg in exg_corr[:self.select_col_num]]
        drop_col = [exg for corr, exg in exg_corr[self.select_col_num:]]
        data_tmp = data_tmp.drop(columns=drop_col)

        return data_tmp, exg_list
