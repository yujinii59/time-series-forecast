import os

from baseline.deployment.pipeline import Pipeline

path_root = os.path.join('..', '..')

# Data Configuration
data_cfg = {'division': 'SELL_IN'}

# Execute Configuration
step_cfg = {
    'cls_load': False,
    'cls_cns': False,    # consistency check
    'cls_proc': True,
    'cls_train': True,
    'cls_pred': True,
    'cls_mdout': True
}

# Configuration
exec_cfg = {
    # save configuration
    'save_step_yn': True,   # Save each step result to object or csv
    'save_db_yn': True,    # Save each step result to Database
}

pipeline = Pipeline(
    step_cfg=step_cfg,
    exec_cfg=exec_cfg,
    path_root=path_root
)

pipeline.run()