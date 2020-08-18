import logging

from datacroaker import RollingReleaseRemoteDataSource
from datacroaker.helper import downloader
from datacroaker.helper.filehandler import unzip

log = logging.getLogger(__name__)


class NcbiTaxonomy(RollingReleaseRemoteDataSource):

    def __init__(self, root_dir):
        super(NcbiTaxonomy, self).__init__(root_dir)

    def download_function(self, instance):
        zip_file = downloader.download_file_to_dir('ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdmp.zip',
                                                   instance.process_instance_dir)
        # unpack zip file
        unzip(zip_file)
