import pronto
import os
import logging
import json
import re

from graphpipeline.parser import ReturnParser
from graphio import NodeSet, RelationshipSet

from biomedgraph.parser.helper.obo import clean_obo_file

log = logging.getLogger(__name__)

class ChebiParser(ReturnParser):

    def __init__(self):
        super(ChebiParser, self).__init__()

        # NodeSets
        self.metabolites = NodeSet(['Metabolite'], merge_keys=['sid'])
        self.metabolite_isa_metabolite = RelationshipSet('IS_A', ['Metabolite'], ['Metabolite'], ['sid'], ['sid'])
        self.metabolite_rel_metabolite = RelationshipSet('CHEBI_REL', ['Metabolite'], ['Metabolite'], ['sid'], ['sid'])
        self.metabolite_maps_metabolite = RelationshipSet('MAPS', ['Metabolite'], ['Metabolite'], ['sid'], ['sid'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        chebi_instance = self.get_instance_by_name('Chebi')

        obo_file = chebi_instance.get_file('chebi.obo')

        cleaned_obo_file = clean_obo_file(obo_file)

        chebi_ontology = pronto.Ontology(cleaned_obo_file)

        reltypes = set()

        # iterate terms
        for term in chebi_ontology.terms():

            term_sid = (term.id).split(':')[1]
            ontology_id = term.id
            self.metabolites.add_node(
                {'name': (term.name), 'sid': term_sid, 'ontology_id': ontology_id,
                 'definition': term.definition, 'alt_ids': list(term.alternate_ids), 'source': 'chebi'}
            )

            for parent in term.superclasses(distance=1, with_self=False):
                self.metabolite_isa_metabolite.add_relationship(
                    {'sid': term_sid}, {'sid': parent.id}, {'source': 'chebi'}
                )

            ## other named relationships
            try:
                for reltype, targets in term.relationships.items():

                    for target in targets:
                        self.metabolite_rel_metabolite.add_relationship(
                            {'sid': term_sid}, {'sid': target.id}, {'source': 'chebi', 'type': reltype.id})
            except KeyError as e:
                log.error(f"Cannot iterate relationshis of term {term_sid}")
                log.error(e)

            # metabolite-MAPS-metabolite
            for xref in term.xrefs:
                if 'HMDB:' in xref.id:
                    hmdb_id = xref.id.strip().split('HMDB:')[1]
                    self.metabolite_maps_metabolite.add_relationship(
                        {'sid': term_sid}, {'sid': hmdb_id}, {'source': 'chebi'}
                    )
