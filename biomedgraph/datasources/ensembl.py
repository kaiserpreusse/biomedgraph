import os
from uuid import uuid4
from datetime import datetime
import requests
import logging

from graphpipeline.datasource import ManyVersionsRemoteDataSource
from graphpipeline.datasource import DataSourceVersion
from graphpipeline.datasource.helper import downloader
from graphpipeline.datasource import DataSourceInstance

log = logging.getLogger(__name__)

ENSEMBL_TAXID_SUBDIR = {
    '9606': 'homo_sapiens',
    '10090': 'mus_musculus'
}

# replace XXVERSIONXX with the current version
# long term solution: complete file name construction including genome name/version
ENSEMBL_TAXID_GTF_FILE = {
    '10090': 'Mus_musculus.GRCm38.XXVERSIONXX.chr_patch_hapl_scaff.gtf.gz',
    '9606': 'Homo_sapiens.GRCh38.XXVERSIONXX.chr_patch_hapl_scaff.gtf.gz'
}

# Homo_sapiens.GRCh38.97.ena.tsv.gz
# Homo_sapiens.GRCh38.97.entrez.tsv.gz
# Homo_sapiens.GRCh38.97.karyotype.tsv.gz
# Homo_sapiens.GRCh38.97.refseq.tsv.gz
# Homo_sapiens.GRCh38.97.uniprot.tsv.gz
ENSEMBL_TAXID_TSV_FILE = {
    '10090': 'Mus_musculus.GRCm38.XXVERSIONXX.XXDBXX.tsv.gz',
    '9606': 'Homo_sapiens.GRCh38.XXVERSIONXX.XXDBXX.tsv.gz'
}


class Ensembl(ManyVersionsRemoteDataSource):


    ENSEMBL_BASEURL = 'ftp://ftp.ensembl.org'
    ENSEMBL_BASEPATH = 'pub'

    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(Ensembl, self).__init__(root_dir)

        self.remote_gtf_urls = {}

    def use_release_str(self, version):
        """
        On the ENSEMBL FTP server, all data for a release is in a subfolder 'release-80'.

        :return: String for release directory.
        :rtype: str
        """
        return "release-{0}".format(version)

    def all_remote_versions(self):
        """
        Get all remote versions from ENSEMBL FTP.

        :return: the list of versions
        :rtype: list of DataSourceVersion
        """
        file_list = downloader.list_ftp_dir(self.ENSEMBL_BASEURL, path=self.ENSEMBL_BASEPATH)

        release_names = [x.name for x in file_list if x.name.startswith('release-')]

        versions = []
        for r in release_names:
            try:
                versions.append(DataSourceVersion(r.split('-')[1]))
            except IndexError:
                pass

        return versions

    def download_function(self, instance, version, taxids=None):
        """
        Download a specific version.

        :param version: The version.
        :param taxids: Optional list of taxonomy IDs to limit download.
        :type version: DataSourceVersion
        """
        self.download_gtf(version, instance.process_instance_dir, taxids)
        self.download_tsv(version, instance.process_instance_dir, taxids)

    def download_gtf(self, version, instance_dir, taxids):
        """
        Download gtf files as primary means of data retrieval.

        :param version: Version
        :type version: DataSourceVersion
        """
        log.info("Download ENSEMBL GTF files.")

        gtfpath = os.path.join(instance_dir, 'gtf')

        if taxids:
            log.info("Filter for {}".format(taxids))
            taxid_2_name, name_2_taxid = self.get_ensembl_species_names_2_taxid()
            allowed_names = [taxid_2_name[taxid] for taxid in taxids]
            for name in allowed_names:
                downloader.download_directory_from_ftp(
                    self.ENSEMBL_BASEURL,
                    '/pub/{}/gtf/{}/'.format(self.use_release_str(version), name),
                    os.path.join(gtfpath, name))

        else:
            log.debug("Download full FTP directory")
            downloader.download_directory_from_ftp(self.ENSEMBL_BASEURL,
                                                   '/{}/{}/gtf/'.format(self.ENSEMBL_BASEPATH,
                                                                        self.use_release_str(version)), gtfpath)

    def download_tsv(self, version, instance_dir, taxids):
        """
        Download TSV directory which contains dumps of common mappings.

        :param version: Version.
        :type version: DataSourceVersion
        """
        log.info("Download ENSEMBL TSV files")

        tsv_path = os.path.join(instance_dir, 'tsv')

        if taxids:
            log.info("Filter for {}".format(taxids))
            taxid_2_name, name_2_taxid = self.get_ensembl_species_names_2_taxid()
            allowed_names = [taxid_2_name[taxid] for taxid in taxids]
            for name in allowed_names:
                downloader.download_directory_from_ftp(
                    self.ENSEMBL_BASEURL,
                    '/pub/{}/tsv/{}/'.format(self.use_release_str(version), name),
                    os.path.join(tsv_path, name),
                    dir_blacklist=['ensembl-compara']
                )

        else:
            log.debug("Download full FTP directory")
            downloader.download_directory_from_ftp(
                self.ENSEMBL_BASEURL, '/pub/{}/tsv/'.format(self.use_release_str(version)), tsv_path,
                dir_blacklist=['ensembl-compara'], dir_whitelist=allowed_names
            )

    @staticmethod
    def get_ensembl_species_names_2_taxid():
        """
        ENSEMBL uses species names such as 'homo_sapiens' and 'mus_musculus. The REST API has
        an endpoint that returns a list of species that contain both the name and the TaxonomyID:

        http://rest.ensembl.org/documentation/info/species

        ENSEMBL has the notion of 'reference strains' and specific strains (e.g. different strains for
        the reference strain 'sus_scrofa'. The `strain` property is either `null` or `reference` or `strain_name`.

        For the dictionaries only the reference strain is used. However, some taxonomyIDs have mulitple
        reference strains. Those taxons are excluded from the dictionary

        {
          "species": [
            {
              "accession": "GCA_000239415.1",
              "groups": [
                "rnaseq",
                "core",
                "otherfeatures"
              ],
              "assembly": "AstBur1.0",
              "division": "EnsemblVertebrates",
              "name": "haplochromis_burtoni",
              "aliases": [],
              "strain": null,
              "taxon_id": "8153",
              "release": 99,
              "strain_collection": null,
              "common_name": "Burton's mouthbrooder",
              "display_name": "Burton's mouthbrooder"
            }, ...
        }

        :return: two dictionaries with taxid->name and name->taxID
        """
        server = "http://rest.ensembl.org"
        ext = "/info/species?"

        r = requests.get(server + ext, headers={"Content-Type": "application/json"})

        decoded = r.json()

        taxid_2_name = {}
        name_2_taxid = {}

        # some taxids have multiple reference strains, those taxids are removed from the dictionary
        duplicate_taxids = set()
        duplicate_names = set()

        for element in decoded['species']:

            name = element.get('name', None)
            taxid = element.get('taxon_id', None)
            strain = element.get('strain', None)
            # set strain to 'reference' if either not found or 'null' in JSON
            if not strain:
                strain = 'reference'

            # only use reference strains (i.e. either no strain value or strain: 'reference'
            if 'reference' in strain:

                if name and taxid:
                    if taxid in taxid_2_name:
                        duplicate_taxids.add(taxid)
                    else:
                        taxid_2_name[taxid] = name

                    if name in name_2_taxid:
                        duplicate_names.add(name)
                    else:
                        name_2_taxid[name] = taxid

        log.debug("Remove taxids with mulitple reference names: {}".format(duplicate_taxids))
        for taxid in duplicate_taxids:
            taxid_2_name.pop(taxid, None)

        return taxid_2_name, name_2_taxid

    @staticmethod
    def get_gtf_file_path(taxid, instance):
        """
        Return the path to a GTF file for a given taxid.

        :param taxid: The taxid
        :param instance: The DataSource instance
        :return: The GTF file path
        """
        version = DataSourceVersion.version_from_string(
            instance.version
        )

        organism_subpath = ENSEMBL_TAXID_SUBDIR[taxid]

        gtf_file_name_base = ENSEMBL_TAXID_GTF_FILE[taxid]
        gtf_file_name = gtf_file_name_base.replace('XXVERSIONXX', str(version))

        gtf_file_path = os.path.join(instance.instance_dir, 'gtf', organism_subpath, gtf_file_name)

        return gtf_file_path

    @staticmethod
    def get_tsv_file_path(taxid, database, instance):
        """
        Return the path to a TSV file for a given taxid and reference database.

        :param taxid: The taxid
        :param database: The reference database
        :param instance: The DataSource instance
        :return: The TSV file path
        """
        version = DataSourceVersion.version_from_string(
            instance.version
        )

        organism_subpath = ENSEMBL_TAXID_SUBDIR[taxid]

        tsv_file_name_base = ENSEMBL_TAXID_TSV_FILE[taxid]
        tsv_file_name = tsv_file_name_base.replace('XXVERSIONXX', str(version)).replace('XXDBXX', database)

        tsv_file_path = os.path.join(instance.instance_dir, 'tsv', organism_subpath, tsv_file_name)

        return tsv_file_path
