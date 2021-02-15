import logging
import requests
import json
import os

from graphpipeline.datasource import RollingReleaseRemoteDataSource
from graphpipeline.datasource.helper import downloader

log = logging.getLogger(__name__)


class OboFoundry(RollingReleaseRemoteDataSource):

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(OboFoundry, self).__init__(root_dir)

    @staticmethod
    def load_ontology_table():
        return json.loads(requests.get("http://www.obofoundry.org/registry/ontologies.jsonld").text)

    def download_all_obo(self, target_dir):
        """
        Download obo files of all active ontologies.

        :param target_dir: The target dir (i.e. instance process dir)
        """

        for ontology in self.load_ontology_table()['ontologies']:
            if ontology['activity_status'] == 'active':
                ontology_id = ontology['id']
                for product in ontology['products']:
                    if '.obo' in product['id'] or '.owl' in product['id']:
                        ontology_target_dir = os.path.join(target_dir, ontology_id)
                        try:
                            downloader.download_file_to_dir(product['ontology_purl'], ontology_target_dir)
                        except Exception as e:
                            log.error(f"Can't download {product['ontology_purl']}, continue with next files.")
                            log.error(e)

    def download_function(self, instance):
        """
        Download latest availbale version.

        The full table of ontologies can be downlaode from http://www.obofoundry.org/registry/ontologies.yml
        """
        log.debug("Download OBO Foundry")

        # download ontology table
        for f in ['http://www.obofoundry.org/registry/ontologies.jsonld',
                  'http://www.obofoundry.org/registry/ontologies.yml']:
            downloader.download_file_to_dir(f, instance.process_instance_dir)

        self.download_all_obo(instance.process_instance_dir)
