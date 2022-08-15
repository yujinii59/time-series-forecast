from baseline.preprocess.init import Init
from baseline.preprocess.load import Load
from baseline.preprocess.proc import Process
from baseline.model.train import Train
from baseline.model.predict import Predict
from common.sql import Sql
from dao.DataIO import DataIO
import common.config as config


class Pipeline:
    def __init__(
            self,
            step_cfg: dict,  # step configuration
            exec_cfg: dict,  # execute configuration
            path_root: str,
    ):
        self.step_cfg = step_cfg
        self.exec_cfg = exec_cfg
        self.path_root = path_root
        self.io = DataIO()
        self.sql = Sql()

    def run(self):
        print('Step 1: Initial Setting')
        init = Init(io=self.io, sql=self.sql, path_root=self.path_root)
        init.run(cust_lvl=config.hrchy_cust_lvl, item_lvl=config.hrchy_item_lvl)

        load = Load(io=self.io, sql=self.sql, data_vrsn=init.data_vrsn, period=init.period, common=init.common)
        if self.step_cfg['cls_load']:
            print('Step 2: Loading Data')
            load_data = load.run()

            # Save Step result
            if self.exec_cfg['save_step_yn']:
                self.io.save_object(data=load_data, data_type='binary', file_path=init.path['load'])

        else:
            load_data = self.io.load_object(file_path=init.path['load'], data_type='binary')

        if self.step_cfg['cls_proc']:
            print('Step 3: Preprocessing')
            process = Process(init=init, data=load_data)

            processed = process.process()
            processed_data, exg_list, hrchy_cnt = processed

            # Save Step result
            if self.exec_cfg['save_step_yn']:
                self.io.save_object(data=processed, data_type='binary', file_path=init.path['preprocess'])
        else:
            processed_data, exg_list, hrchy_cnt = self.io.load_object(file_path=init.path['preprocess'],
                                                                      data_type='binary')

        if self.step_cfg['cls_train']:
            print('Step 4: Evaluation')
            train = Train(io=self.io,
                          sql=self.sql,
                          data=processed_data,
                          init=init,
                          common=init.common,
                          mst_info=load_data['master'],
                          exg_list=exg_list
                        )

            models = train.training()

            # Save Step result
            if self.exec_cfg['save_step_yn']:
                self.io.save_object(data=models, data_type='binary', file_path=init.path['train'])
        else:
            models = self.io.load_object(file_path=init.path['train'], data_type='binary')

        if self.step_cfg['cls_pred']:
            print('Step5: Prediction')
            pred = Predict(io=self.io,
                           sql=self.sql,
                           init=init,
                           common=init.common,
                           data=processed_data,
                           models=models,
                           mst_info=load_data['master'],
                           exg_list=exg_list
                        )

            prediction = pred.predict()

        self.io.session.close()
