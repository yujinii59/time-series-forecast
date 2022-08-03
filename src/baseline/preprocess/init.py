import common.util as util
import common.config as config


class Init(object):
    def __init__(self, io, sql, path_root):
        self.io = io
        self.sql = sql
        self.path_root = path_root
        self.data_vrsn = ''
        self.period = {}
        self.midout = True  # Apply middle-out or not
        self.level = {}
        self.hrchy = {}
        self.path = {}

    def get_data_version(self):
        # from M4S_I110420 table
        data_vrsn_info = self.io.get_df_from_db(sql=self.sql.sql_data_version())
        self.data_vrsn = data_vrsn_info['data_vrsn_cd'][0]
        self.period = {
            'from': data_vrsn_info['from_date'][0],
            'to': data_vrsn_info['to_date'][0]
        }

    def run(self, cust_lvl, item_lvl):
        self.get_data_version()
        self.set_data_level(cust_lvl=cust_lvl, item_lvl=item_lvl)
        self.set_hrchy()
        self.set_path()

    def set_data_level(self, cust_lvl, item_lvl):
        self.level = {
            'cust_lvl': cust_lvl,
            'item_lvl': item_lvl,
            'midout': self.midout
        }

    def set_hrchy(self):
        self.hrchy = {
            'key': 'C' + str(self.level['cust_lvl']) + '-' + 'P' + str(self.level['item_lvl']),
            'recur_lvl': {
                'cust': self.level['cust_lvl'],
                'item': self.level['item_lvl'],
                'total': self.level['cust_lvl'] + self.level['item_lvl']
            },
            'list': {
                'cust': config.hrchy_cust,
                'item': config.hrchy_item
            },
            'apply': config.hrchy_cust[:self.level['cust_lvl']] + config.hrchy_item[:self.level['item_lvl']]
        }

    # save step object & result (binary / csv)
    def set_path(self):
        # each step (load / preprocess / train / predict / middle-out)
        self.path = {
            'load': util.make_path(
                path=self.path_root, module='data', data_vrsn=self.data_vrsn, step='load', extension='pickle'
            ),
            'preprocess': util.make_path(
                path=self.path_root, module='result', data_vrsn=self.data_vrsn, step='preprocess', extension='pickle'
            ),
            'train': util.make_path(
                path=self.path_root, module='result', data_vrsn=self.data_vrsn, step='train', extension='pickle'
            ),
            'predict': util.make_path(
                path=self.path_root, module='result', data_vrsn=self.data_vrsn, step='predict', extension='pickle'
            ),
            'midout': util.make_path(
                path=self.path_root, module='result', data_vrsn=self.data_vrsn, step='midout', extension='pickle'
            )
        }


