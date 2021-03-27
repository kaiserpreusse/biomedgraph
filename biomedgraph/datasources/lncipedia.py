import logging

from graphpipeline.datasource import SingleVersionRemoteDataSource
from graphpipeline.datasource.datasourceversion import DataSourceVersion
from graphpipeline.datasource.helper.downloader import download_file_to_dir

log = logging.getLogger(__name__)


class Lncipedia(SingleVersionRemoteDataSource):

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(Lncipedia, self).__init__(root_dir)

        self.remote_files = [
            'https://lncipedia.org/downloads/lncipedia_5_2/full-database/lncipedia_5_2_hg38.gff',
            'https://lncipedia.org/downloads/lncipedia_5_2/high-confidence-set/lncipedia_5_2_hc_hg38.gff'
        ]

    def latest_remote_version(self):
        """
        Only the latest version is accessible.
        """
        return DataSourceVersion('5.2')

    def download_function(self, instance, version):
        files = ['https://lncipedia.org/downloads/lncipedia_5_2/full-database/lncipedia_5_2_hg38.gff',
                 'https://lncipedia.org/downloads/lncipedia_5_2/high-confidence-set/lncipedia_5_2_hc_hg38.gff']

        for f in files:
            download_file_to_dir(f, instance.process_instance_dir)