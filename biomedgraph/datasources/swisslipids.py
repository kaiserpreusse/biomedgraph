from datetime import datetime

from graphpipeline.datasource import RollingReleaseRemoteDataSource
from graphpipeline.datasource.helper import downloader


class SwissLipids(RollingReleaseRemoteDataSource):

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(SwissLipids, self).__init__(root_dir)

    def download_function(self, instance):
        files = [
            ('https://www.swisslipids.org/api/file.php?cas=download_files&file=enzymes.tsv', 'enzymes.tsv.gz'),
            ('https://www.swisslipids.org/api/file.php?cas=download_files&file=tissues.tsv','tissues.tsv.gz'),
            ('https://www.swisslipids.org/api/file.php?cas=download_files&file=go.tsv', 'go.tsv'),
            ('https://www.swisslipids.org/api/file.php?cas=download_files&file=evidences.tsv', 'evidences.tsv.gz'),
            ('https://www.swisslipids.org/api/file.php?cas=download_files&file=lipids2uniprot.tsv', 'lipids2uniprot.tsv.gz'),
            ('https://www.swisslipids.org/api/file.php?cas=download_files&file=lipids.tsv', 'lipids.tsv.gz')
        ]

        for file, name in files:
            downloader.download_file_to_dir(file, instance.process_instance_dir, filename=name)
