from datetime import datetime

from datacroaker import ManyVersionsRemoteDataSource
from datacroaker import DataSourceVersion
from datacroaker.helper import downloader
from datacroaker import DataSourceInstance

VERSION_2_URL = {'6': 'http://mirdb.org/download/miRDB_v6.0_prediction_result.txt.gz',
                 '5': 'http://mirdb.org/download/miRDB_v5.0_prediction_result.txt.gz'}
VERSION_2_FILENAME = {'6': 'miRDB_v6.0_prediction_result.txt.gz',
                      '5': 'miRDB_v5.0_prediction_result.txt.gz'}


class Mirdb(ManyVersionsRemoteDataSource):
    """
    ftp://mirbase.org/pub/mirbase/21/
    ftp://mirbase.org/pub/mirbase
    """

    def __init__(self, root_dir):
        super(Mirdb, self).__init__(root_dir)

    def all_remote_versions(self):
        """
        Get all versions available on miRBase FTP server.

        All versions are a directory in /pub/mirbase.

        :return: List of DataSourceVersions available on server.
        :rtype: list(DataSourceVersion)
        """
        versions = [DataSourceVersion(x) for x in VERSION_2_URL]
        return versions

    def download_function(self, instance, version):
        downloader.download_file_to_dir(VERSION_2_URL[str(version)], instance.process_instance_dir)

    @staticmethod
    def get_prediction_file(instance):
        version_string = instance.version
        return instance.get_file(VERSION_2_FILENAME[version_string])
