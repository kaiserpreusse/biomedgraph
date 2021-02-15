from datetime import datetime

from graphpipeline.datasource import RollingReleaseRemoteDataSource
from graphpipeline.datasource import DataSourceInstance
from graphpipeline.datasource.helper import downloader


class NcbiGene(RollingReleaseRemoteDataSource):

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(NcbiGene, self).__init__(root_dir)

    def download_function(self, instance):
        files = [
            'ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz',
            'ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Mus_musculus.gene_info.gz',
            'ftp://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz',
            'ftp://ftp.ncbi.nih.gov/gene/DATA/gene2ensembl.gz',
            'ftp://ftp.ncbi.nih.gov/gene/DATA/gene2accession.gz',
            'ftp://ftp.ncbi.nih.gov/gene/DATA/gene_orthologs.gz'
        ]

        for file in files:
            downloader.download_file_to_dir(file, instance.process_instance_dir)
