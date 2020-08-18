from datacroaker import SingleVersionRemoteDataSource
from datacroaker import DataSourceVersion
from datacroaker.helper import downloader

FILE_URL_FORMAT_VERSION = 'http://mirtarbase.mbc.nctu.edu.tw/cache/download/{}/miRTarBase_MTI.xlsx'


class Mirtarbase(SingleVersionRemoteDataSource):

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(Mirtarbase, self).__init__(root_dir)

    def latest_remote_version(self):
        """
        Only the latest version is accessible.
        """
        return DataSourceVersion('7.0')

    def download_function(self, instance, version):
        file = FILE_URL_FORMAT_VERSION.format(str(version))
        downloader.download_file_to_dir(file, instance.process_instance_dir)
