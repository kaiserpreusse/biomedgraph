from lxml import etree
import logging

from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)


class HmdbParser(ReturnParser):

    def __init__(self, root_dir):
        super(HmdbParser, self).__init__(root_dir)

        # NodeSets
        self.metabolites = NodeSet(['Metabolite'], merge_keys=['sid'])

        self.metabolite_map_metabolite = RelationshipSet('MAPS', ['Metabolite'], ['Metabolite'], ['sid'], ['sid'])
        self.metabolite_associates_protein = RelationshipSet('HAS_ASSOCIATION', ['Metabolite'], ['Protein'], ['sid'],
                                                             ['sid'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self, *args, **kwargs):

        hmdb_instance = self.get_instance_by_name('Hmdb')

        all_metabolites_file = hmdb_instance.get_file('hmdb_metabolites.xml')

        all_metabolites = etree.parse(all_metabolites_file)

        for metabolite in all_metabolites.getroot():
            # TODO just iterate over property list, this code snippet was copied from manually testing stuff in Spyder
            # TODO filter empty properties
            sid = metabolite.findtext('{http://www.hmdb.ca}accession')
            name = metabolite.findtext('{http://www.hmdb.ca}name')
            chebi_id = metabolite.findtext('{http://www.hmdb.ca}chebi_id')
            chemspider_id = metabolite.findtext('{http://www.hmdb.ca}chemspider_id')
            cs_description = metabolite.findtext('{http://www.hmdb.ca}cs_description')
            description = metabolite.findtext('{http://www.hmdb.ca}description')
            chemical_formula = metabolite.findtext('{http://www.hmdb.ca}chemical_formula')
            average_molecular_weight = metabolite.findtext('{http://www.hmdb.ca}average_molecular_weight')
            iupac_name = metabolite.findtext('{http://www.hmdb.ca}iupac_name')
            cas_registry_number = metabolite.findtext('{http://www.hmdb.ca}cas_registry_number')
            smiles = metabolite.findtext('{http://www.hmdb.ca}smiles')
            inchi = metabolite.findtext('{http://www.hmdb.ca}inchi')
            kegg_id = metabolite.findtext('{http://www.hmdb.ca}kegg_id')

            metabolite_properties = {
                'sid': sid,
                'name': name,
                'chebi_id': chebi_id,
                'chemspider_id': chemspider_id,
                'cs_description': cs_description,
                'description': description,
                'chemical_formula': chemical_formula,
                'average_molecular_weight': average_molecular_weight,
                'iupac_name': iupac_name,
                'cas_registry_number': cas_registry_number,
                'smiles': smiles,
                'inchi': inchi,
                'kegg_id': kegg_id,
                'source': 'hmdb'
            }

            self.metabolites.add_node(metabolite_properties)

            # add mapping to Chebi
            if chebi_id:
                self.metabolite_map_metabolite.add_relationship(
                    {'sid': sid}, {'sid': chebi_id}, {'source': 'hmdb'}
                )

            # add association to Proteins
            for protein in metabolite.find('{http://www.hmdb.ca}protein_associations'):
                uniprot_id = protein.findtext('{http://www.hmdb.ca}uniprot_id')
                self.metabolite_associates_protein.add_relationship(
                    {'sid': sid}, {'sid': uniprot_id}, {'source': 'hmdb'}
                )
