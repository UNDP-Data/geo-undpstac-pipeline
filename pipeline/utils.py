import os
def mkdir_recursive(path):
    """
        make dirs in the path recursively
        :param path:
        :return:
    """

    sub_path = os.path.dirname(path)
    if not os.path.exists(sub_path):
        mkdir_recursive(sub_path)
    if not os.path.exists(path):
        os.mkdir(path)