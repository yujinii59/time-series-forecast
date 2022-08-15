import common.config as config
import common.util as util
from baseline.model.algorithm import Algorithm

import numpy as np
import pandas as pd
from copy import deepcopy
import datetime


class Predict(object):
    estimators = {
        'ar': Algorithm.ar,
        'arima': Algorithm.arima,
        'hw': Algorithm.hw,
        'var': Algorithm.var,
        'varmax': Algorithm.varmax,
        'sarima': Algorithm.sarimax,
        'prophet': Algorithm.prophet
    }

    def __init__(self, io, sql, init, common, data, models, mst_info, exg_list):
        self.io = io
        self.sql = sql
        self.init = init
        self.data_vrsn = init.data_vrsn
        self.rst_end_day = init.period['to']
        self.recur_lvl = self.init.hrchy['recur_lvl']
        self.hrchy_lvl = self.recur_lvl['total']
        self.hrchy_list = self.init.hrchy['apply'] + ['yymmdd', 'week']
        self.common = common
        self.data = data
        self.model = models
        self.mst_info = mst_info
        self.models = mst_info[config.key_model]
        self.calendar_data = mst_info[config.key_calendar_mst]
        self.exg_list = exg_list

    def predict(self):
        pred = util.drop_down_hrchy_data_model(
            data=self.data,
            models=self.model,
            hrchy=[],
            fn=self.pred_model,
            lvl=0,
            hrchy_lvl=self.hrchy_lvl - 1
        )
        return pred

    def pred_model(self, data: pd.DataFrame, model: tuple, hrchy: list):
        feature = self.feature_setting(data=data)
        estimate_result = {}
        models = self.models[self.models['model'] == model[0]]
        hyperparam = model[1]
        variate = models['variate'].values[0]
        train = self.set_train(
            feature=feature[variate],
            variate=variate,
            label_width=models['label_width'].values[0]
        )

        pred = self.predict_target_week(
            model=model[0],
            hyperparam=hyperparam,
            label_width=models['label_width'].values[0],
            train=train
        )

        pred_date = self.set_pred_yymmdd(
            cal=self.calendar_data,
            end_day=self.rst_end_day,
            label_width=models['label_width'].values[0]
        )

        df = self.save_pred(df=pred_date, pred=pred, model=model[0], hrchy=hrchy)

        return df

    def feature_setting(self, data: pd.DataFrame):
        seq = data['qty'].to_numpy()
        tmp = deepcopy(data)
        tmp = tmp.drop(columns=self.hrchy_list)
        multi = tmp.to_numpy()
        feature = {
            'univ': seq,
            'multi': {
                'exog': multi,
                'endog': seq
            }
        }

        return feature

    @staticmethod
    def set_train(feature: dict, variate: str, label_width: int):
        if variate == 'univ':
            train = feature
        else:
            train = {
                'exog': feature['exog'][:-label_width],
                'endog': feature['endog'][:-label_width]
            }

        return train

    def predict_target_week(self, model: str, hyperparam: dict, label_width: int, train: np.array):
        rst = self.estimators[model](history=train, cfg=hyperparam, pred_step=label_width)

        return list(rst)

    def set_pred_yymmdd(self, cal: pd.DataFrame, end_day: str, label_width: int):
        pred_day = datetime.datetime.strptime(end_day, '%Y%m%d') + datetime.timedelta(days=1)
        date = {'yymmdd': [], 'week': []}
        for _ in range(label_width):
            str_pred_day = datetime.datetime.strftime(pred_day, '%Y%m%d')
            date['yymmdd'].append(str_pred_day)
            week = cal[cal['yymmdd'] == str_pred_day]['week'].values[0]
            date['week'].append(week)
            pred_day = pred_day + datetime.timedelta(days=7)

        df = pd.DataFrame(date)

        return df

    def save_pred(self, df: pd.DataFrame, pred: np.array, model: str, hrchy: list):
        df['result_sales'] = [round(rst, 3) for rst in pred]
        df['stat_cd'] = model.upper()
        df['project_cd'] = 'ENT001'
        df['data_vrsn_cd'] = self.data_vrsn
        df['division_cd'] = 'SELL_IN'
        df['fkey'] = 'C' + str(self.recur_lvl['cust']) + '-' + \
                     'P' + str(self.recur_lvl['item']) + '-' + \
                     hrchy[0] + '-' + hrchy[-1]

        db_hrchy = self.common['db_hrchy_item_cd'].split(',')
        db_hrchy = [self.common['hrchy_cust']] + db_hrchy
        db_hrchy = db_hrchy[:self.hrchy_lvl]

        for db, h in zip(db_hrchy, hrchy):
            df[db] = h

        self.io.insert_to_db(df=df, tb_name='M4S_O110600')

        return df
