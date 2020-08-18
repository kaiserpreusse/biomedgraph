from datetime import datetime, date

from datacroaker import SingleVersionRemoteDataSource
from datacroaker import DataSourceVersion
from datacroaker.helper import downloader
from datacroaker import DataSourceInstance


class Gtex(SingleVersionRemoteDataSource):


    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(Gtex, self).__init__(root_dir)

    def latest_remote_version(self):
        """
        Only the latest version is accessible.
        """
        return DataSourceVersion('8')

    def download_function(self, instance, version):
        """
        Download latest version.
        """

        files = [
            'https://storage.googleapis.com/gtex_analysis_v8/annotations/GTEx_Analysis_v8_Annotations_SampleAttributesDD.xlsx',
            'https://storage.googleapis.com/gtex_analysis_v8/annotations/GTEx_Analysis_v8_Annotations_SubjectPhenotypesDD.xlsx',
            'https://storage.googleapis.com/gtex_analysis_v8/annotations/GTEx_Analysis_v8_Annotations_SampleAttributesDS.txt',
            'https://storage.googleapis.com/gtex_analysis_v8/annotations/GTEx_Analysis_v8_Annotations_SubjectPhenotypesDS.txt',
            'https://storage.googleapis.com/gtex_analysis_v8/rna_seq_data/GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_median_tpm.gct.gz'
            # 'https://storage.googleapis.com/gtex_analysis_v8/rna_seq_data/GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_tpm.gct.gz',
            # 'https://storage.googleapis.com/gtex_analysis_v8/rna_seq_data/GTEx_Analysis_2017-06-05_v8_RSEMv1.3.0_transcript_tpm.gct.gz'
        ]

        for f in files:
            downloader.download_file_to_dir(f, instance.process_instance_dir)
