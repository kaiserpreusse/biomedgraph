from datetime import datetime
from uuid import uuid4
import logging

from datacroaker import SingleVersionRemoteDataSource
from datacroaker import DataSourceVersion
from datacroaker.helper import downloader
from datacroaker import DataSourceInstance

log = logging.getLogger(__name__)


class Mesh(SingleVersionRemoteDataSource):

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(Mesh, self).__init__(root_dir)

    def latest_remote_version(self):
        """
        Only the latest version is accessible.
        """
        return DataSourceVersion('2020')

    def download_function(self, instance, version):
        """
        Download latest version.
        """

        downloader.download_file_to_dir('ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/xmlmesh/desc2020.xml',
                                        instance.process_instance_dir)
        downloader.download_file_to_dir('ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/xmlmesh/qual2020.xml',
                                        instance.process_instance_dir)
        downloader.download_file_to_dir('ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/xmlmesh/supp2020.xml',
                                        instance.process_instance_dir)
