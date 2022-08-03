import os


def make_path(path, module, step, data_vrsn, extension):
    path_dir = os.path.join(path, module, data_vrsn)
    if not os.path.isdir(path_dir):
        os.makedirs(path_dir)

    file_path = os.path.join(path_dir, data_vrsn + '_' + step + '.' + extension)

    return file_path
