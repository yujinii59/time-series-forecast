import common.config as config
import common.util as util
from baseline.model.algorithm import Algorithm

import numpy as np
import pandas as pd
from typing import Any
from itertools import product


class Train(object):
    estimators = {
        'ar': Algorithm.ar,
        'arima': Algorithm.arima,
        'hw': Algorithm.hw,
        'var': Algorithm.var,
        'varmax': Algorithm.varmax,
        'sarima': Algorithm.sarimax,
        'prophet': Algorithm.prophet
    }

    def __init__(self, io, sql, data, init, common, mst_info, exg_list, hrchy_cnt):
        self.io = io
        self.sql = sql
        self.data = data
        self.init = init
        self.hrchy_lvl = self.init.hrchy['recur_lvl']['total']
        self.common = common
        self.mst_info = mst_info
        self.models = mst_info[config.key_model]
        self.exg_list = exg_list
        self.hrchy_cnt = hrchy_cnt

    def training(self):
        models = util.drop_down_hrchy_data(data=self.data, fn=self.train_model, lvl=0, hrchy_lvl=self.hrchy_lvl - 1,
                                           cnt=0)

    def train_model(self, data: pd.DataFrame):
        feature = self.feature_setting(data=data)
        estimate_result = {}
        for i, models in self.models.iterrows():
            model = models['model']
            feat = feature[models['variate']]
            train, test = self.train_test_split(feature=feat, eval_width=models['eval_width'],
                                                label_width=models['label_width'])
            estimate_result[model] = self.estimate_target_week(
                model=model,
                cfg=self.mst_info[config.key_hyper_param][model],
                eval_width=models['eval_width'],
                train=train,
                test=test
            )

        estimate_result['voting'] = sum(list(estimate_result.values())) / len(estimate_result)
        estimate_result = sorted(estimate_result.items(), key=lambda x: x[1])

    def feature_setting(self, data: pd.DataFrame):
        seq = data['qty'].to_numpy()
        col = data.columns[self.hrchy_lvl:]
        multi = data[col].to_numpy()
        feature = {
            'univ': seq,
            'multi': {
                'exog': multi,
                'endog': seq
            }
        }

        return feature

    @staticmethod
    def train_test_split(feature: np.array, eval_width: int, label_width: int):
        if len(feature.shape) == 1:
            train = feature[:-eval_width]
            test = feature[-label_width:]
        else:
            train = {
                'exog': feature['exog'][:-eval_width],
                'endog': feature['endog'][:-eval_width]
            }
            test = feature['endog'][-label_width:]

        return train, test

    def estimate_target_week(self, model: str, cfg: dict, eval_width: int, train: Any, test: pd.Series):
        cases = self.grid_search_func(cfg=cfg)
        min_rmse = int(1e9)
        min_case = {}
        for case in cases:
            rst = self.estimators[model](history=train, cfg=case, pred_step=eval_width)
            rmse = self.calculate_rmse(estimate=rst, test=test)
            if min_rmse > rmse:
                min_rmse = rmse
                min_case = case

        self.save_hyper_params(model=model, hyper_param=min_case)

        return min_rmse

    @staticmethod
    def grid_search_func(cfg: dict):
        cases = ['']
        for k, v in cfg.items():
            cases = list(product(v, cases))

        param_case = []
        for case in cases:
            params = {}
            for k in list(cfg.keys())[::-1]:
                params[k] = case[0]
                case = case[1]
            param_case.append(params)

        return param_case

    @staticmethod
    def calculate_rmse(estimate, test):
        l = len(test)
        ms = 0
        for esti, tgt in zip(estimate, test):
            ms += (tgt - esti) ** 2

        rmse = (ms / l) ** 0.5

        return rmse

    def save_hyper_params(self, model: str, hyper_param: dict):
        kwargs = {
            'project_cd': 'ENT001',
            'stat_cd': model.upper(),
            'option_cd': tuple(hyper_param.keys())
        }
        self.io.delete_from_db(sql=self.sql.del_hyper_params(**kwargs))

        df = pd.DataFrame([[option.upper(), val] for option, val in hyper_param.items()],
                          columns=['OPTION_CD', 'OPTION_VAL'])
        df['PROJECT_CD'] = 'ENT001'
        df['STAT_CD'] = model.upper()
        self.io.insert_to_db(df=df, tb_name='M4S_I103011')

        print("")