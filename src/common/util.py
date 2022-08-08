import os


def make_path(path, module, step, data_vrsn, extension):
    path_dir = os.path.join(path, module, data_vrsn)
    if not os.path.isdir(path_dir):
        os.makedirs(path_dir)

    file_path = os.path.join(path_dir, data_vrsn + '_' + step + '.' + extension)

    return file_path


def drop_down_hrchy_data(data, fn=None, lvl=0, hrchy_lvl=3):
    hrchy_data = {}
    for key, val in data.items():
        if lvl < hrchy_lvl:
            hrchy_data[key] = drop_down_hrchy_data(data=val, fn=fn,
                                                   lvl=lvl+1, hrchy_lvl=hrchy_lvl)

        else:
            hrchy_data[key] = fn(data=val)

    return hrchy_data
