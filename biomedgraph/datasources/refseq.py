import posixpath
import os

from datacroaker import ManyVersionsRemoteDataSource
from datacroaker import DataSourceVersion
from datacroaker.helper import downloader


class Refseq(ManyVersionsRemoteDataSource):
    BASEURL = 'ftp://ftp.ncbi.nlm.nih.gov'
    BASEPATH = 'refseq/release/release-catalog/'

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(Refseq, self).__init__(root_dir)

    def latest_remote_version(self):
        """
        Get number of latest release from ftp://ftp.ncbi.nlm.nih.gov/refseq/release/RELEASE_NUMBER.
        :return: DataSourceVersion of latest remote version
        """
        # returns a BytesIO object
        release_file = downloader.get_single_file_ftp('ftp://ftp.ncbi.nlm.nih.gov/refseq/release/RELEASE_NUMBER')
        # to get the text simply 'read' and 'decode'
        release_number = int(release_file.read().decode().rstrip())
        return DataSourceVersion(release_number)

    def all_remote_versions(self):
        """
        Get all Versions from catalogue archive plus latest version.
        :return: list of variables of the type DataSourceVersion
        """

        # get from release catalogue archive
        archive_path = posixpath.join(self.BASEURL, self.BASEPATH, 'archive')

        file_list = downloader.list_ftp_dir(archive_path)
        # get names of release catalogue files
        release_catalog_list = set([
            x.name for x in file_list if 'RefSeq-release' in x.name
        ])

        # get release numbers from names
        # RefSeq-release20.catalog.gz => 20
        version_numbers = [
            int(x.split('-')[1].split('.')[0].replace('release', '')) for x in release_catalog_list
        ]

        # put in DataSourceVersion, add latest
        ds_versions = [DataSourceVersion(x) for x in version_numbers]
        ds_versions.append(self.latest_remote_version())

        return ds_versions

    def release_path(self, version):
        """
        Get path to used release. This is either the base path or the sub directory 'archive'.
        :param version: variable of the type DataSourceVersion
        :return: Path for current release.
        """
        # path is either base for latest version or 'archive' if not latest
        if version == self.latest_remote_version():
            cat_path = posixpath.join(self.BASEURL, self.BASEPATH)
        else:
            cat_path = posixpath.join(self.BASEURL, self.BASEPATH, 'archive')

        return cat_path

    def download(self, instance, version):
        """
        Download the catalogue file containing all RefSeq ids and the gene transcript mapping file.
        """

        if self.version_downloadable(version):
            cat_file_name = 'RefSeq-release{0}.catalog.gz'.format(version)
            downloader.download_file_to_dir(
                posixpath.join(self.release_path(version), cat_file_name), instance.process_instance_dir
            )

            filename = 'release{0}.accession2geneid.gz'.format(version)
            downloader.download_file_to_dir(
                posixpath.join(self.release_path(version), filename), instance.process_instance_dir
            )

    @staticmethod
    def get_catalog_file_path(instance):
        """
        Return the path to the Catalog file for a given instance.

        :param instance: The DataSource instance
        :return: The Catlog file path
        """
        version = DataSourceVersion.version_from_string(
            instance.version
        )

        file_name = 'RefSeq-release{0}.catalog.gz'.format(version)
        file_path = os.path.join(instance.instance_dir, file_name)

        return file_path

    @staticmethod
    def get_accession2geneid_file_path(instance):
        """
        Return the path to the Catalog file for a given instance.

        :param instance: The DataSource instance
        :return: The Catlog file path
        """
        version = DataSourceVersion.version_from_string(
            instance.version
        )

        file_name = 'release{0}.accession2geneid.gz'.format(version)
        file_path = os.path.join(instance.instance_dir, file_name)

        return file_path
