from datacroaker import RollingReleaseRemoteDataSource
from datacroaker.helper import downloader


class HGNC(RollingReleaseRemoteDataSource):


    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(HGNC, self).__init__(root_dir)

    def download_function(self, instance):
        files = [
            'ftp://ftp.ebi.ac.uk/pub/databases/genenames/new/tsv/hgnc_complete_set.txt',
        ]

        for file in files:
            downloader.download_file_to_dir(file, instance.process_instance_dir)
