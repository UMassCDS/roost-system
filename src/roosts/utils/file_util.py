import os


# not exist dir, then create
def mkdir(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)


def delete_files(filepaths):
    for filepath in filepaths:
        if os.path.exists(filepath):
            os.remove(filepath)
