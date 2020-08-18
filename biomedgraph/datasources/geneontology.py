import logging

from datacroaker import RollingReleaseRemoteDataSource
from datacroaker.helper import downloader

log = logging.getLogger(__name__)

# GeneOntology association subsets are available for some organisms
# located at `/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz`
TAXID_2_ORG_DIR_NAME = {
    '9606': 'HUMAN',
    '10090': 'MOUSE'
}

TAXID_2_ORG_FILE_NAME = {
    '9606': 'human',
    '10090': 'mouse'
}


class GeneOntology(RollingReleaseRemoteDataSource):


    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(GeneOntology, self).__init__(root_dir)

    def download_function(self, instance, taxids=None):
        """
        Download a specific version.

        There are subsets of the data for some key organisms.

        :param version: The version.
        :param taxids: List of taxIDs to download files for
        :type version: DataSourceVersion
        """
        log.debug("Download GO Annotation")

        if taxids and all(taxid in TAXID_2_ORG_DIR_NAME for taxid in taxids):
            log.debug("Download subsets for organisms")
            for taxid in taxids:
                this_taxid_dirname = TAXID_2_ORG_DIR_NAME[taxid]
                this_taxid_filename = TAXID_2_ORG_FILE_NAME[taxid]
                files = [
                    'ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/{0}/goa_{1}.gaf.gz'.format(this_taxid_dirname,
                                                                                         this_taxid_filename),
                    'ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/{0}/goa_{1}.gpa.gz'.format(this_taxid_dirname,
                                                                                         this_taxid_filename),
                    'ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/{0}/goa_{1}.gpi.gz'.format(this_taxid_dirname,
                                                                                         this_taxid_filename),
                ]
                for file in files:
                    downloader.download_file_to_dir(file, instance.process_instance_dir)

        else:
            files = [
                'ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/UNIPROT/goa_uniprot_all.gaf.gz',
                'ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/UNIPROT/goa_uniprot_all.gpa.gz',
                'ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/UNIPROT/goa_uniprot_all.gpi.gz',
            ]

            for file in files:
                downloader.download_file_to_dir(file, instance.process_instance_dir)
