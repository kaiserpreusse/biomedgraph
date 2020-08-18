from datetime import datetime, date
import posixpath
from xml.etree import ElementTree

from datacroaker import SingleVersionRemoteDataSource
from datacroaker import DataSourceVersion
from datacroaker.helper import downloader
from datacroaker import DataSourceInstance

TAXID_KB_FILE_NAME = {
    '9606': 'human',
    '10090': 'rodents'
}


class Uniprot(SingleVersionRemoteDataSource):
    UNIPROT_BASEURL = 'ftp://ftp.ebi.ac.uk'
    UNIPROT_PREVIOUS_BASEPATH = '/pub/databases/uniprot/previous_releases/'
    UNIPROT_CURRENT_BASEPATH = 'pub/databases/uniprot/current_release/'

    def __init__(self, root_dir):
        super(Uniprot, self).__init__(root_dir)

    def latest_remote_version(self):
        """
        Versions are named with 'year_month', e.g. '2016_01'.
        Data is parsed from 'RELEASE.metalink' file in 'current_release' directory of FTP server.

        :return: Latest remote version.
        :rtype: DataSourceVersion
        """
        current_release_path = posixpath.join(self.UNIPROT_BASEURL, self.UNIPROT_CURRENT_BASEPATH)

        release_file = posixpath.join(current_release_path, 'RELEASE.metalink')

        # read XML file from server into string
        release_metalink_xml = downloader.get_single_file_ftp(release_file).read().decode()

        # replace xml namespace thing to avoid dealing with namespaces
        release_metalink_xml = release_metalink_xml.replace(' xmlns="', ' xmlnamespace="')

        # parse XML
        tree = ElementTree.fromstring(release_metalink_xml)
        version = tree.find('version').text

        return DataSourceVersion(self._date_from_name(version))

    def use_release_str(self, version):
        """
        On the Uniprot FTP server, all data for a release is in a subfolder 'release-2017_06s'.

        :return: String for release directory.
        :rtype: str
        """
        return "release-{0}".format(version)

    # previous versions are packed into a single file and cannot be handled like the current version
    # this function gives a list of all releases but old releases cannot be downloaded like the current one
    # but packed into one big data file. this is useless for now.
    # def all_remote_versions(self):
    #     """
    #     Get all remote versions from Uniprot FTP.
    #     """
    #     file_list = downloader.list_ftp_dir(self.UNIPROT_BASEURL, path=self.UNIPROT_PREVIOUS_BASEPATH)
    #
    #     release_names = [x.name for x in file_list if 'release' in x.name]
    #
    #     versions = []
    #     for r in release_names:
    #         try:
    #             versions.append(DataSourceVersion(r.split('-')[1]))
    #         except IndexError:
    #             pass
    #
    #     versions.append(
    #         self.latest_remote_version()
    #     )
    #
    #     return versions

    def download_function(self, instance, version):

        if self.version_downloadable(version):
            self._download_latest_taxonomic_division(instance)

    # TODO refactor
    # def _download_latest_complete(self, version):
    #
    #     downloader.download_file_to_dir(
    #         '{}/{}knowledgebase/complete/uniprot_sprot.dat.gz'.format(self.UNIPROT_BASEURL,
    #                                                                   self.UNIPROT_CURRENT_BASEPATH),
    #         self.version_dir(version))
    #     downloader.download_file_to_dir(
    #         '{}/{}knowledgebase/complete/uniprot_trembl.dat.gz'.format(self.UNIPROT_BASEURL,
    #                                                                    self.UNIPROT_CURRENT_BASEPATH),
    #         self.version_dir(version))

    def _download_latest_taxonomic_division(self, instance):
        # download division files
        for division in ['human', 'rodents']:
            downloader.download_file_to_dir(
                '{}/{}knowledgebase/taxonomic_divisions/uniprot_sprot_{}.dat.gz'.format(
                    self.UNIPROT_BASEURL,
                    self.UNIPROT_CURRENT_BASEPATH,
                    division),
                instance.process_instance_dir)
            downloader.download_file_to_dir(
                '{}/{}knowledgebase/taxonomic_divisions/uniprot_trembl_{}.dat.gz'.format(
                    self.UNIPROT_BASEURL,
                    self.UNIPROT_CURRENT_BASEPATH,
                    division),
                instance.process_instance_dir)

    # previous versions are packed into a single file and cannot be handled like the current version
    # this function downloads the main file for a specific previous version but the file is useless for now
    # def _download_previous_all(self, version):
    #
    #     base = '{}{}{}knowledgebase/knowledgebase'.format(self.UNIPROT_BASEURL, self.UNIPROT_PREVIOUS_BASEPATH,
    #                                                       self.use_release_str(version)) + version + '.tar.gz'
    #     downloader.download_file_to_dir(base,
    #                                     self.version_dir(version))

    @staticmethod
    def _date_from_name(name):
        """
        Get datetime.date from release name (e.g. 'release-2014_01' or '2015_01')
        :param name: name of release
        :type name: str
        :return: datetime.date of release (with day 1)
        :rtype: datetime.date
        """
        # handle dir names starting with 'release'
        if name.startswith('release-') and len(name.split('-')) == 2:
            name = name.split('-')[1]

        year, month = name.split('_')
        return date(int(year), int(month), 1)

    @staticmethod
    def get_knowledgebase_files_for_taxid(taxid, instance):
        """

        :param taxid: The reference taxid
        :type taxid: str
        :param instance: The DataSourceInstance
        :type instance: DataSourceInstance
        """

        sprot_file_name = 'uniprot_sprot_{}.dat.gz'.format(TAXID_KB_FILE_NAME[taxid])
        trembl_file_name = 'uniprot_trembl_{}.dat.gz'.format(TAXID_KB_FILE_NAME[taxid])

        return [instance.get_file(sprot_file_name), instance.get_file(trembl_file_name)]
