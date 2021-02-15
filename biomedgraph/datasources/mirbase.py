import os

from graphpipeline.datasource import ManyVersionsRemoteDataSource
from graphpipeline.datasource import DataSourceVersion
from graphpipeline.datasource.helper import downloader


class Mirbase(ManyVersionsRemoteDataSource):
    mirbase_baseurl = "ftp://mirbase.org"
    mirbase_basepath = '/pub/mirbase'

    def __init__(self, root_dir):
        super(Mirbase, self).__init__(root_dir)

    def all_remote_versions(self):
        """
        Get all versions available on miRBase FTP server.

        All versions are a directory in /pub/mirbase.

        :return: List of DataSourceVersions available on server.
        :rtype: list(DataSourceVersion)
        """
        ftp_list = downloader.list_ftp_dir(self.mirbase_baseurl, path=self.mirbase_basepath)
        versions = [DataSourceVersion(x.name) for x in ftp_list]

        return versions

    def download_function(self, instance, version):
        mirbase_version_path = os.path.join(self.mirbase_basepath, str(version))
        downloader.download_directory_from_ftp(self.mirbase_baseurl, mirbase_version_path,
                                               instance.process_instance_dir)
