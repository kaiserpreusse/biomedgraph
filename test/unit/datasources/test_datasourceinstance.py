import pytest
import os
from collections import namedtuple

from biodatagraph.datasources.datasourceinstance import DataSourceInstance

def test_find_files_in_directory(tmp_path):
    DataSource = namedtuple('DataSource', ['ds_dir'])
    test_datasource = DataSource(tmp_path)

    dsi = DataSourceInstance(test_datasource)

    # create stuff in instance dir
    os.makedirs(os.path.join(dsi.instance_dir, 'mouse'))
    os.makedirs(os.path.join(dsi.instance_dir, 'human'))

    with open(os.path.join(dsi.instance_dir, 'mouse', 'data.txt'), 'a'):
        os.utime(os.path.join(dsi.instance_dir, 'mouse', 'data.txt'), None)

    with open(os.path.join(dsi.instance_dir, 'human', 'data.txt'), 'a'):
        os.utime(os.path.join(dsi.instance_dir, 'human', 'data.txt'), None)

    get_file = dsi.get_file_from_directory('human', 'data.txt')
    assert 'human' in get_file
    assert 'mouse' not in get_file