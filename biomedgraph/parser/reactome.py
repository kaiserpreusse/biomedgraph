import logging

from elderberry import ReturnParser
from biomedgraph.parser.helper.taxtranslator import TaxTranslator
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)


class ReactomePathwayParser(ReturnParser):

    def __init__(self, root_dir):
        """

        :param mirtarbase_instance: NcbiGene Instance
        :type mirtarbase_instance: DataSourceInstance
        """
        super(ReactomePathwayParser, self).__init__(root_dir)

        # NodeSets
        self.pathways = NodeSet(['Pathway'], merge_keys=['sid'])

        # RelationshipSets
        self.pathway_child_pathway = RelationshipSet('CHILD', ['Pathway'], ['Pathway'], ['sid'], ['sid'])

        self.object_sets = [self.pathways, self.pathway_child_pathway]
        self.container.add_all(self.object_sets)

        self._taxtranslator = None

    @property
    def taxtranslator(self):
        if not self._taxtranslator:
            self._taxtranslator = TaxTranslator(self.get_instance_by_name('NcbiTaxonomy'))
            log.info(self._taxtranslator)
        return self._taxtranslator

    @property
    def reactome_instance(self):
        return self.get_instance_by_name('Reactome')

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        log.debug("Run {}".format(self.__class__.__name__))
        self.run_pathways()
        self.run_pathway_child()

    def run_pathway_child(self):
        pathway_structure_file = self.reactome_instance.get_file('ReactomePathwaysRelation.txt')
        log.debug("Pathway relations file: {}".format(pathway_structure_file))

        with open(pathway_structure_file, 'rt') as f:
            for l in f:
                flds = l.strip().split('\t')
                left = flds[0]
                right = flds[1]
                self.pathway_child_pathway.add_relationship({'sid': left}, {'sid': right},
                                                            {'source': self.reactome_instance.datasource.name})

    def run_pathways(self):
        pathway_file = self.reactome_instance.get_file('ReactomePathways.txt')
        log.debug("Pathway file: {}".format(pathway_file))
        with open(pathway_file, 'rt') as f:
            for l in f:
                flds = l.strip().split('\t')
                sid = flds[0]
                name = flds[1]
                org = flds[2]
                taxid = str(self.taxtranslator.translate(org))

                self.pathways.add_node(
                    {'taxid': taxid, 'sid': sid, 'name': name, 'org': org,
                     'source': self.reactome_instance.datasource.name}
                )


class ReactomeMappingParser(ReturnParser):
    """
    Data for mapping entities to pathways is in a set of simple mapping files provided by Reactome.

    NCBI2Reactome_All_Levels.txt, Ensembl2Reactome_All_Levels.txt etc.

    These files have 5 fields:

    - external ID
    - pathway ID
    - pathway URI
    - pathway name
    - evidence code
    - organism name (Homo sapiens, Mus musculus etc)

    The parser iterates these files and creates mapping relationships.

    The TaxTranslator is used to get the TaxID for the organism name.
    """


    def __init__(self, root_dir):
        """

        :param mirtarbase_instance: NcbiGene Instance
        :type mirtarbase_instance: DataSourceInstance
        """
        super(ReactomeMappingParser, self).__init__(root_dir)

        # arguments
        self.arguments = ['taxid']

        # RelationshipSets
        self.gene_member_pathway = RelationshipSet('MEMBER', ['Gene'], ['Pathway'], ['sid'], ['sid'])

        self.object_sets = [self.gene_member_pathway]
        self.container.add_all(self.object_sets)

        self._taxtranslator = None

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    @property
    def taxtranslator(self):
        if not self._taxtranslator:
            self._taxtranslator = TaxTranslator(self.get_instance_by_name('NcbiTaxonomy'))
            log.info(self._taxtranslator)
        return self._taxtranslator

    @property
    def reactome_instance(self):
        return self.get_instance_by_name('Reactome')

    def run(self, taxid):
        self.run_ensembl_gene_pathway_mapping(taxid)
        self.run_ncbi_gene_pathway_mapping(taxid)

    def run_ensembl_gene_pathway_mapping(self, reference_taxid):

        mapping_file = self.reactome_instance.get_file('Ensembl2Reactome_All_Levels.txt')

        with open(mapping_file, 'rt') as f:
            for l in f:
                flds = l.strip().split('\t')
                ensembl_id = flds[0]
                reactome_id = flds[1]
                evidence_code = flds[4]
                org = flds[5].strip()

                taxid = str(self.taxtranslator.translate(org))

                if taxid == reference_taxid:
                    self.gene_member_pathway.add_relationship(
                        {'sid': ensembl_id}, {'sid': reactome_id}, {'source': 'reactome', 'evidence': evidence_code}
                    )

    def run_ncbi_gene_pathway_mapping(self, reference_taxid):

        mapping_file = self.reactome_instance.get_file('NCBI2Reactome_All_Levels.txt')

        with open(mapping_file, 'rt') as f:
            for l in f:
                flds = l.strip().split('\t')
                ncbi_gene_id = flds[0]
                reactome_id = flds[1]
                evidence_code = flds[4]
                org = flds[5].strip()

                taxid = str(self.taxtranslator.translate(org))

                if taxid == reference_taxid:
                    self.gene_member_pathway.add_relationship(
                        {'sid': ncbi_gene_id}, {'sid': reactome_id}, {'source': 'reactome', 'evidence': evidence_code}
                    )
