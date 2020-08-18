import logging
import gzip

from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)

TAXID_2_ORG_FILE_NAME = {
    '9606': 'human',
    '10090': 'mouse'
}


class GeneOntologyAssociationParser(ReturnParser):
    """
    Parse GeneOntology Associations from the official UniProt association files.

    There are three different files available:

    - goa_uniprot_all.gaf.gz
    - goa_uniprot_all.gpa.gz
    - goa_uniprot_all.gpi.gz

    The GPA (Gene Product Association) file contains one gene product - GO Term tuple per line. There is additional
    information about the gene products in the GPI (Gene Product Information) file which augments the GPA file.

    The GAF file merges GPA and GPI (by adding the gene product information to each line with an association)
    and thus contains a lot of redundant information on the gene product.

    The GAF file is parsed for the mappings because it contains the mapping as well as the taxonomy ID. It is
    easier to iterate one file instead of generating a gene product - taxonomy ID mapping from the GPI file and
    then read the GPA file.

    More information from the header of the GPA file:

    !This file contains all GO annotations for proteins in the UniProt KnowledgeBase (UniProtKB).
    !
    !It also contains all annotations for protein complexes, identified by ComplexPortal identifiers,
    !and for non-coding RNAs, identified by RNAcentral identifiers
    !
    !Columns:
    !
    !   name                  required? cardinality   GAF column #
    !   DB                    required  1             1
    !   DB_Object_ID          required  1             2 / 17
    !   Qualifier             required  1 or greater  4
    !   GO ID                 required  1             5
    !   DB:Reference(s)       required  1 or greater  6
    !   ECO evidence code     required  1             7 (GO evidence code)
    !   With                  optional  0 or greater  8
    !   Interacting taxon ID  optional  0 or 1        13
    !   Date                  required  1             14
    !   Assigned_by           required  1             15
    !   Annotation Extension  optional  0 or greater  16
    !   Annotation Properties optional  0 or 1        n/a

    And from the header of the GPI file:

    !This file contains additional information for proteins in the UniProt KnowledgeBase (UniProtKB).
    !Protein accessions are represented in this file even if there is no associated GO annotation.
    !
    !Columns:
    !
    !   name                   required? cardinality   GAF column #  Example content
    !   DB                     required  1             1             UniProtKB
    !   DB_Object_ID           required  1             2/17          Q4VCS5-1
    !   DB_Object_Symbol       required  1             3             AMOT
    !   DB_Object_Name         optional  0 or greater  10            Angiomotin
    !   DB_Object_Synonym(s)   optional  0 or greater  11            AMOT|KIAA1071
    !   DB_Object_Type         required  1             12            protein
    !   Taxon                  required  1             13            taxon:9606
    !   Parent_Object_ID       optional  0 or 1        -             UniProtKB:Q4VCS5
    !   DB_Xref(s)             optional  0 or greater  -             WB:WBGene00000035
    !   Properties             optional  0 or greater  -             db_subset=Swiss-Prot|target_set=KRUK,BHFL

    """


    def __init__(self, root_dir):
        super(GeneOntologyAssociationParser, self).__init__(root_dir)

        self.arguments = ['taxid']

        # RelationshipSets
        self.protein_associates_goterm = RelationshipSet('ASSOCIATION', ['Protein'], ['Term'], ['sid'], ['sid'])

        self.object_sets = [self.protein_associates_goterm]
        self.container.add_all(self.object_sets)

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, ref_taxid):
        go_instance = self.get_instance_by_name('GeneOntology')
        log.debug("Run for {}".format(ref_taxid))
        if ref_taxid in TAXID_2_ORG_FILE_NAME:
            goa_uniprot_gaf_file_name = 'goa_{0}.gaf.gz'.format(TAXID_2_ORG_FILE_NAME[ref_taxid])
            goa_uniprot_gaf_file = go_instance.get_file(goa_uniprot_gaf_file_name)
        else:
            goa_uniprot_gaf_file = go_instance.get_file('goa_uniprot_all.gaf.gz')

        self.parse_goa_uniprot_gaf_file(goa_uniprot_gaf_file, ref_taxid)

    def parse_goa_uniprot_gaf_file(self, goa_uniprot_gaf_file, ref_taxid):

        with gzip.open(goa_uniprot_gaf_file, 'rt') as f:
            for line in f:
                if not line.startswith('!'):
                    line = line.strip()
                    flds = line.split('\t')
                    db = flds[0]

                    try:
                        taxid = flds[12].split(':')[1]
                    except IndexError:
                        continue

                    if taxid == ref_taxid:
                        if db == 'UniProtKB':
                            db_id = flds[1]
                            qualifier = flds[3]
                            go_id = flds[4]
                            evidence = flds[6]

                            rel_properties = {'evidence': evidence}
                            if qualifier:
                                rel_properties['qualifier'] = qualifier

                            self.protein_associates_goterm.add_relationship(
                                {'sid': db_id}, {'sid': go_id}, rel_properties
                            )
