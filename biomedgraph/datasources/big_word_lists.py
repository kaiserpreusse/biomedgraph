"""
Curated English word list from https://www.keithv.com/software/wlist/

He is a Prof at Michigan Technological University.
His "broader interests include human-computer interaction (HCI), speech and language processing...".

He builds intersections from public word lists. You can download combined files for words which appear in
1, 2, ..., N of the intersected word lists.
"""
from datetime import datetime, date
from uuid import uuid4

from graphpipeline.datasource import SingleVersionRemoteDataSource
from graphpipeline.datasource import DataSourceVersion
from graphpipeline.datasource.helper import downloader
from graphpipeline.datasource.helper.filehandler import unzip
from graphpipeline.datasource import DataSourceInstance


BASE_URL = "https://www.keithv.com/software/wlist/wlist_match{}.zip"

class BigWordList(SingleVersionRemoteDataSource):


    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(BigWordList, self).__init__(root_dir)

    def latest_remote_version(self):
        """
        Only the latest version is accessible.
        """
        return DataSourceVersion('03-2018')

    def download_function(self, instance, version):
        """
        Download latest version.
        """
        #for i in range(3, 13):
        for i in range(3, 13):
            download_url = BASE_URL.format(i)
            zip_file_path = downloader.download_file_to_dir(download_url, instance.process_instance_dir)
            unzip(zip_file_path)
