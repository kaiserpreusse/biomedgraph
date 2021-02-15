from datetime import datetime

from graphpipeline.datasource import RollingReleaseRemoteDataSource
from graphpipeline.datasource import DataSourceInstance
from graphpipeline.datasource.helper import downloader


class Reactome(RollingReleaseRemoteDataSource):

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(Reactome, self).__init__(root_dir)

    def download_function(self, instance):
        files = ['https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt',
                 'https://reactome.org/download/current/ChEBI2Reactome_All_Levels.txt',
                 'https://reactome.org/download/current/Ensembl2Reactome_All_Levels.txt',
                 'https://reactome.org/download/current/miRBase2Reactome_All_Levels.txt',
                 'https://reactome.org/download/current/NCBI2Reactome_All_Levels.txt',
                 'https://reactome.org/download/current/ReactomePathways.txt',
                 'https://reactome.org/download/current/ReactomePathwaysRelation.txt']

        for f in files:
            downloader.download_file_to_dir(f, instance.process_instance_dir)
