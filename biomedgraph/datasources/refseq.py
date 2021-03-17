import posixpath
import os
import gzip
from collections import defaultdict
import zlib
import logging

from typing import List

from graphpipeline.datasource import ManyVersionsRemoteDataSource
from graphpipeline.datasource import DataSourceVersion
from graphpipeline.datasource.helper import downloader

log = logging.getLogger(__name__)


def download_and_filter_data_file(url: str, path: str, taxids: List[str]) -> str:
    """
    Most RefSeq data files start with the Taxonomy ID. This function downloads a gzipped
    data file, filters all records for a list of Taxonomy IDs and deletes the original file.

    :param url: URL to download.
    :param path: Local download path.
    :param taxids: List of Taxonomy IDs to filter.
    :return: Path of filtered file.
    """
    taxids = set(taxids)

    downloaded_file = downloader.download_file_to_dir(url, path)

    original_filename = downloaded_file.split('/')[-1]
    downloaded_path = downloaded_file.rsplit('/', 1)[0]

    # release10.removed-records.gz -> release10.removed-records_filtered.gz
    new_filename = f"{original_filename.rsplit('.', 1)[0]}.filtered.gz"
    new_filepath = os.path.join(downloaded_path, new_filename)

    with gzip.open(new_filepath, 'wt') as output:

        with gzip.open(downloaded_file, 'rt') as f:
            try:
                for l in f:
                    this_taxid = l.strip().split('\t')[0]
                    if this_taxid in taxids:
                        output.write(l)
            except zlib.error as e:
                log.error(e)
                log.error(f"File {downloaded_file} not readable, corrupted download.")

    os.remove(downloaded_file)
    return new_filepath


def get_list_of_archived_releases() -> defaultdict:
    """
    The Refseq release has 3 main files:
        - Release catalog
        - accession2geneid
        - removed records

    This function returns a dictionary of archived files by release.
    """
    archive_path = 'ftp://ftp.ncbi.nlm.nih.gov/refseq/release/release-catalog/archive/'
    list_of_archive_files = downloader.list_files_only_ftp_dir(archive_path)

    release_number_2_files = defaultdict(dict)

    for item in list_of_archive_files:
        name = item.name
        if name.startswith('release') and 'accession2geneid' in name:
            # release98.accession2geneid.gz -> 98
            release_number = (name.split('.')[0]).replace('release', '')
            release_number_2_files[release_number]['accession2geneid'] = archive_path+name
        elif name.startswith('release') and 'removed-records' in name:
            # release11.removed-records.gz -> 11
            release_number = (name.split('.')[0]).replace('release', '')
            release_number_2_files[release_number]['removed-records'] = archive_path+name

    return release_number_2_files


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
        arguments = ['taxid']

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

    def download_function(self, instance, version, taxids=None):
        """
        Download the catalogue file containing all RefSeq ids and the gene transcript mapping file.
        """

        self.download_archived_releases(instance.process_instance_dir, taxids)

        catalog_path = 'ftp://ftp.ncbi.nlm.nih.gov/refseq/release/release-catalog/'

        catalog = posixpath.join(catalog_path, f'RefSeq-release{version}.catalog.gz')
        accession2geneid = posixpath.join(catalog_path, f'release{version}.accession2geneid.gz')
        removed_records = f'ftp://ftp.ncbi.nlm.nih.gov/refseq/release/release-catalog/release{version}.removed-records.gz'

        if taxids:
            download_and_filter_data_file(catalog, instance.process_instance_dir, taxids)
            download_and_filter_data_file(accession2geneid, instance.process_instance_dir, taxids)
            download_and_filter_data_file(removed_records, instance.process_instance_dir, taxids)
        else:
            downloader.download_file_to_dir(catalog, instance.process_instance_dir)
            downloader.download_file_to_dir(accession2geneid, instance.process_instance_dir)
            downloader.download_file_to_dir(removed_records, instance.process_instance_dir)


    def download_archived_releases(self, path, taxids=None):
        archive_files = get_list_of_archived_releases()

        for release, files in archive_files.items():
            # only download if accession2geneid and removed-records are available
            if taxids:
                try:
                    download_and_filter_data_file(files['accession2geneid'], path, taxids)
                except KeyError: pass
                try:
                    download_and_filter_data_file(files['removed-records'], path, taxids)
                except KeyError: pass
            else:
                try:
                    downloader.download_file_to_dir(files['accession2geneid'], path)
                except KeyError: pass
                try:
                    downloader.download_file_to_dir(files['removed-records'], path)
                except KeyError: pass

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
