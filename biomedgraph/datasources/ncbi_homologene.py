from datetime import datetime

from datacroaker import SingleVersionRemoteDataSource
from datacroaker import DataSourceInstance
from datacroaker.helper import downloader


class NcbiHomoloGene(SingleVersionRemoteDataSource):
    """
    NCBI HomoloGene is a database that contains groups of homologous genes. The homology is calculated
    based on protein sequences of the latest genome releases. The protein alignments are mapped back to
    the DNA sequences to identify homologous genes.

    The process is described here: https://www.ncbi.nlm.nih.gov/homologene/build-procedure/

    The latest release is from 2014 but it includes the current versions of the human and mouse genome.
    However, it does not include the latest patch level.

    In principle, old builds are available. Right now we only use the 'current' release because we would
    need to track the different input genomes used for the alignments to make sense of the older builds.

    The file `homolgene.data` is a tab separated list of homology groups.

    Fields: group ID, tax ID, gene ID, gene symbol, unclear?, refseq ID

    Example::

    3	9606	34	ACADM	4557231	NP_000007.1
    3	9598	469356	ACADM	160961497	NP_001104286.1
    3	9544	705168	ACADM	109008502	XP_001101274.1
    3	9615	490207	ACADM	545503811	XP_005622188.1

    """
    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(NcbiHomoloGene, self).__init__(root_dir)

    def download_function(self, instance, version):
        downloader.download_file_to_dir(
            'ftp://ftp.ncbi.nih.gov/pub/HomoloGene/current/homologene.data',
            instance.process_instance_dir)
