import pronto
import os
import logging
import json
import re

from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet

from biomedgraph.parser.helper.obo import clean_obo_file

log = logging.getLogger(__name__)

class ChebiParser(ReturnParser):

    def __init__(self, root_dir):
        super(ChebiParser, self).__init__(root_dir)

        # NodeSets
        self.metabolites = NodeSet(['Metabolite'], merge_keys=['sid'])
        self.metabolite_isa_metabolite = RelationshipSet('IS_A', ['Metabolite'], ['Metabolite'], ['sid'], ['sid'])
        self.metabolite_rel_metabolite = RelationshipSet('CHEBI_REL', ['Metabolite'], ['Metabolite'], ['sid'], ['sid'])

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
                 'definition': term.definition, 'alt_ids': list(term.alternate_ids)}
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
        print(reltypes)

    def __parse_obo_file(self, ontology_file):
        """
        Parse an OBO file from OboFoundry and extract Ontology, Terms, Subsets and relationships.

        :param ontology_file: Path to the ontology file.
        """
        this_ontology = pronto.Ontology(ontology_file)

        check_synonym_nodes = set()

        metadata = this_ontology.metadata

        # construct Ontology node
        ontology_sid = metadata.ontology
        ontology_dict = {'sid': ontology_sid, 'date': metadata.date, 'version': metadata.data_version}
        for annotation in metadata.annotations:
            property = annotation.property

            value = None
            if isinstance(annotation, pronto.LiteralPropertyValue):
                value = annotation.literal
            elif isinstance(annotation, pronto.ResourcePropertyValue):
                value = annotation.resource

            if value:
                ontology_dict[property] = value

        self.ontologies.add_node(ontology_dict)

        # construct subset nodes
        for subsetdef in metadata.subsetdefs:
            subset_dict = {'name': subsetdef.name, 'description': subsetdef.description}
            self.subsets.add_node(subset_dict)
            self.subset_of_ontology.add_relationship({'name': subsetdef.name}, {'sid': ontology_sid},
                                                     {'source': 'obofoundry'})

        # iterate terms
        for term in this_ontology.terms():

            term_sid = term.id
            self.terms.add_node(
                {'name': (term.name), 'sid': term_sid, 'namespace': (term.namespace), 'obsolete': term.obsolete,
                 'definition': term.definition, 'alt_ids': list(term.alternate_ids)}
            )

            # term in ontology relationship
            self.term_in_ontology.add_relationship({'sid': term_sid}, {'sid': ontology_sid}, {'source': 'obofoundry'})

            # subset relationships
            for subset_name in term.subsets:
                self.term_in_subset.add_relationship({'sid': term_sid}, {'name': subset_name}, {'source': 'obofoundry'})

            # inter Term relationships
            ## SubClassOf / is_a relationships
            for parent in term.superclasses(distance=1, with_self=False):
                self.term_is_a_term.add_relationship(
                    {'sid': term_sid}, {'sid': parent.id}, {'source': 'obofoundry'}
                )

            ## synonyms
            for synonym in term.synonyms:
                # create synonym node
                if synonym.description not in check_synonym_nodes:
                    self.synonym_terms.add_node({'name': synonym.description})
                self.term_synonym_term.add_relationship(
                    {'sid': term_sid}, {'name': synonym.description},
                    {'source': 'obofoundry', 'scope': synonym.scope, 'xrefs': [xref.id for xref in synonym.xrefs]}
                )

            ## other named relationships
            try:
                for reltype, targets in term.relationships.items():
                    for target in targets:
                        self.term_ontorel_term.add_relationship(
                            {'sid': term_sid}, {'sid': target.id}, {'source': 'obofoundry', 'type': reltype.id})
            except KeyError as e:
                log.error(f"Cannot iterate relationshis of term {term_sid}")
                log.error(e)