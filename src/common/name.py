from common import config


class Key(object):
    def __init__(self):
        self.item_mst = config.key_item_mst
        self.cust_mst = config.key_cust_mst
        self.calendar_mst = config.key_calendar_mst
        self.sales_matrix = config.key_sales_matrix
        self.model = config.key_model
        self.hyper_param = config.key_hyper_param
        self.sell_in = config.key_sell_in
        self.exg_data = config.key_exg_data
        self.best_hyper_param = config.key_best_hyper_param
