import os
from datacroaker import RollingReleaseRemoteDataSource
from datacroaker.helper import downloader


class Chebi(RollingReleaseRemoteDataSource):

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(Chebi, self).__init__(root_dir)

    def download_function(self, instance):
        files = [
            'ftp://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo',
            'ftp://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.owl.gz'
        ]

        for file in files:
            downloader.download_file_to_dir(file, instance.process_instance_dir)

        downloader.download_directory_from_ftp('ftp://ftp.ebi.ac.uk', '/pub/databases/chebi/Flat_file_tab_delimited/',
                                               os.path.join(instance.process_instance_dir, 'tables'))
