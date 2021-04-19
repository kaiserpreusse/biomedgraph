import gzip
import logging

from graphpipeline.parser import ReturnParser
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
    def __init__(self):

        super(RefseqEntityParser, self).__init__()

        # arguments
        self.arguments = ['taxid']

        # define NodeSet and RelationshipSet
        self.transcripts = NodeSet(['Transcript'], merge_keys=['sid'], default_props={'source': 'refseq'})
        self.proteins = NodeSet(['Protein'], merge_keys=['sid'], default_props={'source': 'refseq'})

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
                                     'taxid': taxid}
                                )

                                check_transcripts.add(refseq_acc)
                        # protein
                        if refseq_acc.startswith('NP') or refseq_acc.startswith('XP'):
                            if refseq_acc not in check_proteins:
                                self.proteins.add_node(
                                    {'sid': refseq_acc, 'version': version, 'status': status,
                                     'length': length,
                                     'taxid': taxid})
                                check_proteins.add(refseq_acc)


class RefseqRemovedRecordsParser(ReturnParser):
    """
    Parse all removed records from the removed_records files.

    The mappings to gene IDs are in the accession2geneid of the *previous* release.

    Simple approach is to collect all files from all previous releases and collect all records (relationships
    are filtered locally).
    """
    def __init__(self):
        super(RefseqRemovedRecordsParser, self).__init__()

        self.arguments = ['taxid']

        self.legacy_ids = set()

        self.legacy_transcripts = NodeSet(['Transcript', 'Legacy'], merge_keys=['sid'], default_props={'source': 'refseq'})
        self.legacy_transcript_now_transcript = RelationshipSet('REPLACED_BY', ['Transcript'], ['Transcript'], ['sid'], ['sid'], default_props={'source': 'refseq'})
        self.legacy_proteins = NodeSet(['Protein', 'Legacy'], merge_keys=['sid'], default_props={'source': 'refseq'})
        self.legacy_protein_now_protein = RelationshipSet('REPLACED_BY', ['Protein'], ['Protein'],
                                                                ['sid'], ['sid'], default_props={'source': 'refseq'})
        self.gene_codes_legacy_transcript = RelationshipSet('CODES', ['Gene'], ['Transcript', 'Legacy'], ['sid'], ['sid'], default_props={'source': 'refseq'})
        self.legacy_transcript_codes_protein = RelationshipSet('CODES', ['Transcript', 'Legacy'], ['Protein'],
                                                               ['sid'], ['sid'], default_props={'source': 'refseq'})

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):
        # get the nodes first, this also creates a set of all legacy IDs
        self.get_legacy_nodes(taxid)
        # then get the relationnships to gene IDs, this uses the set of legacy IDs to not recreate existing relationships
        self.get_legacy_gene_rels(taxid)

    def get_legacy_nodes(self, taxid):
        """
        ==========================================
        release#.removed-records
        ==========================================
        Content: Tab-delimited report of records that were included in the previous
        release but are not included in the current release.

        Columns:
         1. taxonomy ID
         2. species name
         3. accession.version
         4. refseq release directory accession is included in
              complete + other directories
              '|' delimited
         5. refseq status
              na - not available; status codes are not applied to most genomic records
              INFERRED
              PREDICTED
              PROVISIONAL
              VALIDATED
              REVIEWED
              MODEL
              UNKNOWN - status code not provided; however usually is provided for
                        this type of record
         6. length
         7. removed status
              dead protein: protein was removed when genomic record was reloaded
                            and protein  was not found on the nucleotide update.
                            This is an implied permanent suppress.

              temporarily suppressed: record was temporarily removed and may be
                                      restored at a later date.

              permanently suppressed: record was permanently removed. It is possible
                                      to restore this type of record however at the
                                      time of removal that action is not anticipated.

              replaced by accession:  the accession in column 3 has become a secondary
                                      accession that cited in column 8.

        :param taxid:
        :return:
        """
        refseq_instance = self.get_instance_by_name('Refseq')

        removed_records_files = refseq_instance.find_files(lambda x: 'removed-records' in x and x.endswith('.gz'))

        for file in removed_records_files:
            log.debug(f"Parse {file}")
            release = file.split('/')[-1].split('.')[0].replace('release', '')
            with gzip.open(file, 'rt') as f:
                for l in f:
                    flds = l.strip().split('\t')
                    this_taxid = flds[0]

                    if this_taxid == taxid:
                        refseq_acc, version = flds[2].split('.')
                        reason = flds[-1]
                        # transcript
                        if refseq_acc.startswith('NM') or refseq_acc.startswith('NR') or refseq_acc.startswith(
                                "XM") or refseq_acc.startswith("XR"):
                            if refseq_acc not in self.legacy_ids:
                                self.legacy_transcripts.add_node(
                                    {'sid': refseq_acc, 'version': version,
                                     'status': 'removed', 'removed_in': release, 'reason': reason,
                                     'taxid': taxid}
                                )

                                self.legacy_ids.add(refseq_acc)

                                if 'replaced by' in reason:
                                    # replaced by NM_022375 -> NM_022375
                                    new_accession = (reason.rsplit(' ', 1)[1]).split('.')[0]
                                    self.legacy_transcript_now_transcript.add_relationship(
                                        {'sid': refseq_acc}, {'sid': new_accession}, {}
                                    )
                        # protein
                        if refseq_acc.startswith('NP') or refseq_acc.startswith('XP'):
                            if refseq_acc not in self.legacy_ids:
                                self.legacy_proteins.add_node(
                                    {'sid': refseq_acc, 'version': version,
                                     'status': 'removed', 'removed_in': release, 'reason': reason,
                                     'taxid': taxid})
                                self.legacy_ids.add(refseq_acc)

                                if 'replaced by' in reason:
                                    # replaced by NM_022375 -> NM_022375
                                    new_accession = (reason.rsplit(' ', 1)[1]).split('.')[0]
                                    self.legacy_protein_now_protein.add_relationship(
                                        {'sid': refseq_acc}, {'sid': new_accession}, {}
                                    )

    def get_legacy_gene_rels(self, taxid):
        """
        Get the gene/protein relationships for the legacy Transcripts.

        ==========================================
        release#.accession2geneid
        ==========================================
        Content: Report of GeneIDs available at the time of the RefSeq release.
        Limited to GeneIDs that are associated with RNA or mRNA records with
        accession prefix N[M|R] and X[M|R].

        Columns (tab delimited):

            1: Taxonomic ID
            2: Entrez GeneID
            3: Transcript accession.version
            4: Protein accession.version
               na if no data
               --for example, the NR_ accession prefix is used for RNA
                 so there is no corresponding protein record

        :param taxid:
        :return:
        """
        log.debug("Get relationships from legacy RefSeq IDs to genes.")
        refseq_instance = self.get_instance_by_name('Refseq')

        archived_accession2geneid = refseq_instance.find_files(lambda x: 'accession2geneid' in x and x.endswith('.gz'))
        check_set = set()
        for file in archived_accession2geneid:
            log.debug(f"Parse {file}")

            with gzip.open(file, 'rt') as f:
                for l in f:
                    flds = l.strip().split('\t')
                    this_taxid = flds[0]
                    if this_taxid == taxid:
                        """
                                    1: Taxonomic ID
            2: Entrez GeneID
            3: Transcript accession.version
            4: Protein accession.version
               na if no data
               --for example, the NR_ accession prefix is used for RNA
                 so there is no corresponding protein record
                        """
                        gene_id = flds[1].strip()
                        transcript_accession = flds[2].strip().split('.')[0]
                        protein_accession = flds[3].strip().split('.')[0]
                        if transcript_accession in self.legacy_ids:
                            if (gene_id, transcript_accession) not in check_set:
                                self.gene_codes_legacy_transcript.add_relationship(
                                    {'sid': gene_id}, {'sid': transcript_accession}, {}
                                )
                                check_set.add((gene_id, transcript_accession))
                            if transcript_accession != 'na':
                                if (transcript_accession, protein_accession) not in check_set:
                                    self.legacy_transcript_codes_protein.add_relationship(
                                        {'sid': transcript_accession}, {'sid': protein_accession}, {}
                                    )
                                    check_set.add((transcript_accession, protein_accession))


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

    def __init__(self):
        """
        :param refseq_instance: The RefSeq DataSource instance.
        """
        super(RefseqCodesParser, self).__init__()

        # arguments
        self.arguments = ['taxid']

        # define NodeSet and RelationshipSet
        self.gene_codes_transcript = RelationshipSet('CODES', ['Gene'], ['Transcript'], ['sid'], ['sid'], default_props={'source': 'refseq'})
        self.transcript_codes_protein = RelationshipSet('CODES', ['Transcript'], ['Protein'], ['sid'], ['sid'], default_props={'source': 'refseq'})

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
                            {'taxid': taxid}
                        )
                        check_g_t_rels.add(gene_id + transcript_id)

                    # the gene/transcript relationship is mostly clear
                    # but often there are no proteins associated
                    if protein_id != 'na':
                        if transcript_id + protein_id not in check_t_p_rels:
                            self.transcript_codes_protein.add_relationship(
                                {'sid': transcript_id},
                                {'sid': protein_id},
                                {'taxid': taxid}
                            )

                            check_t_p_rels.add(transcript_id + protein_id)
