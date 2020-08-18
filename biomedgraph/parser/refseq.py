import gzip
import logging

from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)


class RefseqEntityParser(ReturnParser):
    """
    Extract mRNA/ncRNA transcripts from RefSeq sequence catalog.

    The file contains one sequence item per line. Sequences can be nucleotides (DNA, RNA) and Proteins/Peptides.

    The prefix of the ID describes the type:

    NM_: mRNA (measured)
    XM_: mRNA (predicted)
    NR_: ncRNA (measured)
    XR_: ncRNA (predicted)
    NP_: Protein (measured)
    XP_: Protein (predicted)

    Example line: TaxID, organism, ID, taxonomy, status, length

        9606    Homo sapiens    NM_000035.3     complete|vertebrate_mammalian   REVIEWED        2426
    """
    def __init__(self, root_dir):

        super(RefseqEntityParser, self).__init__(root_dir)

        # arguments
        self.arguments = ['taxid']

        # define NodeSet and RelationshipSet
        self.transcripts = NodeSet(['Transcript'], merge_keys=['sid'])
        self.proteins = NodeSet(['Protein'], merge_keys=['sid'])

        self.object_sets = [self.transcripts, self.proteins]
        self.container.add_all(self.object_sets)

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):

        refseq_instance = self.get_instance_by_name('Refseq')
        refseq_catalog_file = refseq_instance.datasource.get_catalog_file_path(refseq_instance)

        datasource_name = refseq_instance.datasource.name

        with gzip.open(refseq_catalog_file, 'rt') as f:

            check_transcripts = set()
            check_proteins = set()

            for l in f:
                flds = l.rstrip().split('\t')

                if len(flds) >= 6:
                    this_taxid = flds[0]

                    if this_taxid == taxid:
                        refseq_acc, version = flds[2].split('.')
                        status = flds[4]
                        length = flds[5]

                        # split by type
                        # transcript
                        if refseq_acc.startswith('NM') or refseq_acc.startswith('NR') or refseq_acc.startswith(
                                "XM") or refseq_acc.startswith("XR"):
                            if refseq_acc not in check_transcripts:
                                self.transcripts.add_node(
                                    {'sid': refseq_acc, 'version': version, 'status': status,
                                     'length': length,
                                     'source': datasource_name, 'taxid': taxid}
                                )

                                check_transcripts.add(refseq_acc)
                        # protein
                        if refseq_acc.startswith('NP') or refseq_acc.startswith('XP'):
                            if refseq_acc not in check_proteins:
                                self.proteins.add_node(
                                    {'sid': refseq_acc, 'version': version, 'status': status,
                                     'length': length,
                                     'source': datasource_name, 'taxid': taxid})
                                check_proteins.add(refseq_acc)


class RefseqCodesParser(ReturnParser):
    """
        Get mappings from NCBI Gene to Refseq transcripts.

        Refseq provides a mapping file that contains a gene-transcript-protein
        mapping per line: release86.accession2geneid.gz

        Example line: TaxID, NCBI Gene ID, RefSeq transcript ID, RefSeq protein ID

            9606    100008586       NM_001098405.2  NP_001091875.1

        :param refseq_mapping_file: The release86.accession2geneid.gz mapping file
        :param taxid: TaxID
        :return: List of (Gene)-[CODES]-(Transcript) Relationships
        """

    def __init__(self, root_dir):
        """
        :param refseq_instance: The RefSeq DataSource instance.
        """
        super(RefseqCodesParser, self).__init__(root_dir)

        # arguments
        self.arguments = ['taxid']

        # define NodeSet and RelationshipSet
        self.gene_codes_transcript = RelationshipSet('CODES', ['Gene'], ['Transcript'], ['sid'], ['sid'])
        self.transcript_codes_protein = RelationshipSet('CODES', ['Transcript'], ['Protein'], ['sid'], ['sid'])

        self.object_sets = [self.gene_codes_transcript, self.transcript_codes_protein]
        self.container.add_all(self.object_sets)

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):
        refseq_instance = self.get_instance_by_name('Refseq')
        datasource_name = refseq_instance.datasource.name

        refseq_accession2geneid_file = refseq_instance.datasource.get_accession2geneid_file_path(refseq_instance)

        # check sets to avoid duplicates
        check_g_t_rels = set()
        check_t_p_rels = set()

        with gzip.open(refseq_accession2geneid_file, 'rt') as f:
            for l in f:
                flds = l.strip().split('\t')

                this_taxid = flds[0]
                gene_id = flds[1]
                transcript_id = flds[2].split('.')[0]
                protein_id = flds[3].split('.')[0]

                if this_taxid == taxid:
                    # gene-transcript or transcript-protein pairs can be duplicate
                    # if e.g. a gene has one transcript which gives rise to two proteins
                    # we thus check for each pair if it was added already
                    if gene_id + transcript_id not in check_g_t_rels:
                        self.gene_codes_transcript.add_relationship(
                            {'sid': gene_id}, {'sid': transcript_id},
                            {'source': datasource_name, 'taxid': taxid}
                        )
                        check_g_t_rels.add(gene_id + transcript_id)

                    # the gene/transcript relationship is mostly clear
                    # but often there are no proteins associated
                    if protein_id != 'na':
                        if transcript_id + protein_id not in check_t_p_rels:
                            self.transcript_codes_protein.add_relationship(
                                {'sid': transcript_id},
                                {'sid': protein_id},
                                {'source': datasource_name, 'taxid': taxid}
                            )

                            check_t_p_rels.add(transcript_id + protein_id)
