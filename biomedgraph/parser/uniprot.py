import gzip
import logging

from graphpipeline.parser import ReturnParser
from graphpipeline.parser import EMBLReaderUniProt
from graphio import NodeSet, RelationshipSet

TAXID_OS_NAME = {'9606': 'Human',
                 '10090': 'Mouse'}

log = logging.getLogger(__name__)


class UniprotKnowledgebaseParser(ReturnParser):
    """

    Uniprot has extensive mapping data to other data sources.

    Data is in the main Uniprot data file (referred to as Uniprot knowledge base).

    Ensembl:
        DR   Ensembl; ENST00000353703; ENSP00000300161; ENSG00000166913. [P31946-1]
        DR   Ensembl; ENST00000372839; ENSP00000361930; ENSG00000166913. [P31946-1]

    Refseq:
        DR   RefSeq; NP_006752.1; NM_006761.4. [P62258-1]

    The mapping parser returns transcript-protein relationships for both ENSEMBL and RefSeq.
    """
    def __init__(self):
        """
        :param uniprot_instance: The Uniprot instance
        :param taxid: The taxid
        """
        super(UniprotKnowledgebaseParser, self).__init__()

        # arguments
        self.arguments = ['taxid']

        # NodeSet
        self.proteins = NodeSet(['Protein'], merge_keys=['sid'], default_props={'source': 'uniprot'})

        # RelationshipSet
        self.protein_primary_protein = RelationshipSet('PRIMARY', ['Protein'], ['Protein'], ['sid'], ['sid'], default_props={'source': 'uniprot'})
        self.transcript_codes_protein = RelationshipSet('CODES', ['Transcript'], ['Protein'], ['sid'], ['sid'], default_props={'source': 'uniprot'})
        self.protein_maps_protein = RelationshipSet('MAPS', ['Protein'], ['Protein'], ['sid'], ['sid'], default_props={'source': 'uniprot'})

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):

        uniprot_instance = self.get_instance_by_name('Uniprot')

        knowledgebase_files = uniprot_instance.datasource.get_knowledgebase_files_for_taxid(taxid, uniprot_instance)
        datasource_name = uniprot_instance.datasource.name

        # get organims name from taxid
        os_string_id = TAXID_OS_NAME[taxid]

        check_protein = set()
        check_p_p_p = set()
        check_t_c_p = set()
        check_p_m_p = set()

        # for now we always run on SPROT and TREMBL
        for kb_file in knowledgebase_files:
            log.debug(f"Parsing {kb_file}")
            with gzip.open(kb_file, 'rt') as f:
                up_parser = EMBLReaderUniProt(f)

                for record in up_parser.records:
                    # check taxon
                    if os_string_id in record['OS']:
                        # acc
                        acc_list = record['AC']
                        primary_acc = acc_list[0]
                        secondary = acc_list[1:]

                        # (Protein)
                        # make primary protein with full data
                        desc = record['DE']
                        rec_name = desc.split(';')[0].split('Full=')[1]

                        primary_props = {'sid': primary_acc, 'name': rec_name, 'desc': desc, 'category': 'primary',
                                         'taxid': taxid}

                        if primary_acc not in check_protein:
                            self.proteins.add_node(primary_props)
                            check_protein.add(primary_acc)

                        for secondary_acc in secondary:
                            if secondary_acc not in check_protein:
                                self.proteins.add_node(
                                    {'sid': secondary_acc, 'category': 'secondary',
                                     'taxid': taxid})
                                check_protein.add(secondary_acc)

                            # (Protein)-[PRIMARY]-(Protein)
                            if frozenset([primary_acc, secondary_acc]) not in check_p_p_p:
                                self.protein_primary_protein.add_relationship(
                                    {'sid': primary_acc}, {'sid': secondary}, {}
                                )
                                check_p_p_p.add(frozenset([primary_acc, secondary_acc]))

                        # (Transcript)-[CODES]-(Protein)
                        # (Protein)-[MAPS]-(Protein)
                        ## RefSeq
                        # ('RefSeq', ['NP_003395']),
                        refseq_mappings = [x[1] for x in record['DR'] if x[0] == 'RefSeq']
                        for map in refseq_mappings:

                            for refseq_id in map:
                                # remove version from refseq ID
                                refseq_id = refseq_id.split('.')[0]
                                second_letter = refseq_id[1]

                                # (Transcript)-[CODES]-(Protein)
                                if second_letter == 'M' or second_letter == 'R':
                                    for uniprot_acc in acc_list:
                                        if refseq_id + uniprot_acc not in check_t_c_p:
                                            self.transcript_codes_protein.add_relationship(
                                                {'sid': refseq_id}, {'sid': uniprot_acc},
                                                {'source': datasource_name}
                                            )
                                            check_t_c_p.add(refseq_id + uniprot_acc)

                                # (Protein)-[MAPS]-(Protein)
                                if second_letter == 'P':
                                    for uniprot_acc in acc_list:
                                        if uniprot_acc + refseq_id not in check_p_m_p:
                                            self.protein_maps_protein.add_relationship(
                                                {'sid': uniprot_acc}, {'sid': refseq_id},
                                                {}
                                            )
                                            check_p_m_p.add(uniprot_acc + refseq_id)

                        ## ensembl
                        ensembl_mappings = [x[1] for x in record['DR'] if x[0] == 'Ensembl']
                        for map in ensembl_mappings:
                            ensembl_transcript_id = map[0]
                            ensembl_protein_id = map[1]

                            for uniprot_acc in acc_list:
                                # (Transcript)-[CODES]-(Protein)
                                if ensembl_transcript_id + uniprot_acc not in check_t_c_p:
                                    self.transcript_codes_protein.add_relationship(
                                        {'sid': ensembl_transcript_id}, {'sid': uniprot_acc},
                                        {}
                                    )
                                    check_t_c_p.add(ensembl_transcript_id + uniprot_acc)

                                # (Protein)-[MAPS]-(Protein)
                                if ensembl_protein_id + uniprot_acc not in check_p_m_p:
                                    self.protein_maps_protein.add_relationship(
                                        {'sid': uniprot_acc}, {'sid': ensembl_protein_id},
                                        {}
                                    )
                                    check_p_m_p.add(ensembl_protein_id + uniprot_acc)
