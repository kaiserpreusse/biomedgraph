import pronto
import os
import logging
import json
import re

from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)

# some ontologies have specific OBO file names (not uberon.obo but basic.obo)
OBO_FILE_MAPPINGS = {
    'uberon': 'basic.obo'
}


class OboFoundryParser(ReturnParser):

    def __init__(self, root_dir):
        super(OboFoundryParser, self).__init__(root_dir)

        self.arguments = ['ontology_name']

        # NodeSets
        self.ontologies = NodeSet(['Ontology'], merge_keys=['sid'])
        self.terms = NodeSet(['Term'], merge_keys=['sid'])
        self.subsets = NodeSet(['OntologySubset'], merge_keys=['name'])
        self.synonym_terms = NodeSet(['SynonymTerm'], merge_keys=['name'])

        # RelationshipSets
        ## between objects
        self.subset_of_ontology = RelationshipSet('SUBSET_OF', ['OntologySubset'], ['Ontology'], ['name'], ['sid'])
        self.term_in_ontology = RelationshipSet('IN_ONTOLOGY', ['Term'], ['Ontology'], ['sid'], ['sid'])
        self.term_in_subset = RelationshipSet('IN_SUBSET', ['Term'], ['OntologySubset'], ['sid'], ['name'])

        ## defines Term to Term relationships
        self.term_is_a_term = RelationshipSet('IS_A', ['Term'], ['Term'], ['sid'], ['sid'])
        self.term_synonym_term = RelationshipSet('HAS_SYNONYM', ['Term'], ['SynonymTerm'], ['sid'], ['name'])
        # self.term_intersection_of_term = RelationshipSet('INTERSECTION_OF', ['Term'], ['Term'], ['sid'], ['sid'])
        # self.term_union_of_term = RelationshipSet('UNION_OF', ['Term'], ['Term'], ['sid'], ['sid'])
        # self.term_disjoint_from_term = RelationshipSet('DISJOINT_FROM', ['Term'], ['Term'], ['sid'], ['sid'])

        ## additional relationships defined in the ontology
        self.term_ontorel_term = RelationshipSet('ONTOREL', ['Term'], ['Term'], ['sid'], ['sid'])

    @property
    def obo_instance(self):
        return self.get_instance_by_name('OboFoundry')

    def parse_obo_file(self, ontology_file):
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

    def clean_obo_file(self, obofile):
        """
        Clean format problems.

        :param obofile: Path to OBO file.
        :return: Path to cleaned file.
        """
        log.debug(f"Clean OBI file {obofile}")
        path = os.path.dirname(obofile)
        filename = os.path.basename(obofile)

        output_filename = "temp_clean_{}".format(filename)
        output = os.path.join(path, output_filename)
        log.debug(f"Write cleaned file to {output}")

        try:
            os.remove(output)
        except Exception as e:
            log.info(e)

        # clean file, dbxref links in def line not well formatted
        first_term_found = False

        with open(output, 'wt') as out:
            with open(obofile, 'rt') as f:
                for l in f:
                    # check if first term was found (= skip header)
                    if not first_term_found:
                        if '[Term]' in l:
                            first_term_found = True
                    if first_term_found:
                        # clean xref def
                        l = self.remove_space_from_xref(l)

                    out.write(l)

        return output

    def remove_space_from_xref(self, line):
        """
        Clean up xref definitions in a way that all spaces are removed or replaced by an underscore.

        - [EC: 1.2.3.4] is bad, no space after EC: [EC:1.2.3.4] is correct
        - [TAO:Arratia and Schultze_1992] is bad, replace space with underscore

        :param line:
        :return:
        """
        # check if line contains xrefs
        line = line.strip()
        # if the line ends with ']]' there are malformatted xrefs
        # such as [ISBN 3-89937-052-X [Goujet and Young\, 2004\]]
        if line.endswith(']]'):
            if '[' in line:
                # get all occurences of '['
                all_open_brackets = [m.start() for m in re.finditer('\[', line)]
                index_second_last_open_bracket = all_open_brackets[-2]
                str_before_bracket = line[:index_second_last_open_bracket]
                str_xref_def = line[index_second_last_open_bracket:]

                line = str_before_bracket + '[' + self.clean_string_xref_element(str_xref_def) + ']'

        elif line.endswith(']') and line != '[Term]' and line != '[Typedef]':
            if '[' in line:
                # find last '[' bracket
                xref_open_bracket_index = line.rfind('[')

                str_before_bracket = line[:xref_open_bracket_index]
                str_xref_def = line[xref_open_bracket_index:]

                # split up the xref def
                cleaned_xref_strings = []

                # first handle lists xref defs with proper key:value
                if ':' in str_xref_def and ', ' in str_xref_def:
                    for xref in str_xref_def[1:-1].split(','):
                        if ':' in xref:
                            cleaned_xref = self.clean_key_value_xref_element(xref)
                            # if there is wrong formatting, the cleaning function returns non
                            if cleaned_xref:
                                cleaned_xref_strings.append(cleaned_xref)
                        else:
                            cleaned_xref = self.clean_string_xref_element(xref)
                            # if there is wrong formatting, the cleaning function returns non
                            if cleaned_xref:
                                cleaned_xref_strings.append(cleaned_xref)

                # then handle single xref defs
                elif ':' in str_xref_def and ', ' not in str_xref_def:
                    cleaned_xref = self.clean_key_value_xref_element(str_xref_def[1:-1])
                    # if there is wrong formatting, the cleaning function returns non
                    if cleaned_xref:
                        cleaned_xref_strings.append(cleaned_xref)

                else:
                    cleaned_xref = self.clean_string_xref_element(str_xref_def[1:-1])
                    if cleaned_xref:
                        cleaned_xref_strings.append(cleaned_xref)

                if cleaned_xref_strings:
                    cleaned_xref_def = ''.join(['[', ', '.join(cleaned_xref_strings), ']'])
                else:
                    cleaned_xref_def = '[]'

                line = str_before_bracket + cleaned_xref_def

        return line + '\n'

    @staticmethod
    def clean_key_value_xref_element(xref):
        """
        Takes an xref element with key and value and cleans it up.

        :param xref: The xref element.
        :return: The cleaned xref element
        """
        xref = xref.strip()

        k, v = xref.split(':', 1)
        # remove ' ' in key
        k = k.replace(' ', '').replace('\\', '')

        # remove starting ' ' in value
        if v.startswith(' '):
            v = v[1:]
        # remove trailing ',' in value
        if v.endswith(','):
            v = v[:-1]
        # replace all other spaces with underscore
        v = v.replace(' ', '_').replace('\\', '')

        # some values contain subvalues (wrong formatting)
        # [GO:[GOC:mtg_sensu, ISBN:0198547684]
        # simply remove those elements
        if not v.startswith('[') and not v.endswith(']'):
            return f"{k}:{v}"

    @staticmethod
    def clean_string_xref_element(xref):

        cleaned_string = xref.strip().replace(' ', '_').replace('\\', '').replace('[', '').replace(']', '')
        if "ISBN" in xref:
            print(xref)
            print(cleaned_string)

        return cleaned_string

    def run_with_mounted_arguments(self):
        self.run(self.ontology_name)

    def run(self, ontology_name):
        """
        Parse a specific ontology by name.
        :param ontology_name: The ontology name (e.g. go, uberon)
        """
        if ontology_name in OBO_FILE_MAPPINGS:
            obo_filename = OBO_FILE_MAPPINGS[ontology_name]
        else:
            obo_filename = "{}.obo".format(ontology_name)

        obo_file_path = self.obo_instance.get_file_from_directory(ontology_name, obo_filename)

        # clean obo files
        cleaned_file = self.clean_obo_file(obo_file_path)

        self.parse_obo_file(cleaned_file)

    def get_obofile_table(self):
        """
        Get the table of OBO files from json file
        :return:
        """
        table_file = self.obo_instance.get_file('ontologies.jsonld')

        return json.loads(table_file)
