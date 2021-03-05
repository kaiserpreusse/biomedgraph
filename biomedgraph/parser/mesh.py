import xml.etree.ElementTree as ET
import logging

from graphpipeline.parser import ReturnParser
from graphpipeline.datasource import DataSourceVersion
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)


class MeshParser(ReturnParser):

    def __init__(self):
        super(MeshParser, self).__init__()

        # NodeSets
        self.descriptor = NodeSet(['MeshDescriptor'], merge_keys=['sid'])
        self.qualifier = NodeSet(['MeshQualifier'], merge_keys=['sid'])
        self.concept = NodeSet(['MeshConcept'], merge_keys=['sid'])
        self.term = NodeSet(['MeshTerm'], merge_keys=['sid'])

        self.descriptor_allowed_qualifier = RelationshipSet('ALLOWED', ['MeshDescriptor'], ['MeshQualifier'], ['sid'],
                                                            ['sid'])

        self.descriptor_has_concept = RelationshipSet('HAS', ['MeshDescriptor'], ['MeshConcept'], ['sid'], ['sid'])
        self.descriptor_has_concept.unique = True
        self.concept_has_term = RelationshipSet('HAS', ['MeshConcept'], ['MeshTerm'], ['sid'], ['sid'])
        self.concept_has_term.unique = True
        self.concept_related_concept = RelationshipSet('RELATED', ['MeshConcept'], ['MeshConcept'], ['sid'], ['sid'])
        self.concept_related_concept.unique = True

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        self.parse_xml()

    def parse_xml(self):
        """
        Parse descriptor XML file.
        """
        mesh_instance = self.get_instance_by_name('Mesh')

        version = DataSourceVersion.version_from_string(
            mesh_instance.version
        )

        descriptor_xml = mesh_instance.get_file('desc{}.xml'.format(str(version)))
        log.debug("XML file {}".format(descriptor_xml))

        tree = ET.parse(descriptor_xml)
        root = tree.getroot()

        check_qualifier = set()
        check_concepts = set()
        check_terms = set()

        for descriptor_record in root.getchildren():
            descriptor_ui = descriptor_record.find('DescriptorUI').text

            # <DescriptorName>
            #  <String>Calcimycin</String>
            # </DescriptorName>
            descriptor_name = descriptor_record.find('.DescriptorName/String').text

            self.descriptor.add_node({'sid': descriptor_ui, 'name': descriptor_name})

            #   <AllowableQualifiersList>
            #   <AllowableQualifier>
            #    <QualifierReferredTo>
            #     <QualifierUI>Q000302</QualifierUI>
            #      <QualifierName>
            #      <String>isolation &amp; purification</String>
            #      </QualifierName>
            #    </QualifierReferredTo>
            #    <Abbreviation>IP</Abbreviation>
            #   </AllowableQualifier>
            #   </AllowableQualifiersList>

            allowed_qualifiers = descriptor_record.findall(
                '.AllowableQualifiersList/AllowableQualifier/QualifierReferredTo')
            for qualifier in allowed_qualifiers:
                qualifier_ui = qualifier.find('.QualifierUI').text

                # add qualifier node id not exists
                if qualifier_ui not in check_qualifier:
                    qualifier_name = qualifier.find('.QualifierName/String').text
                    self.qualifier.add_node({'sid': qualifier_ui, 'name': qualifier_name})
                    check_qualifier.add(qualifier_ui)

                # add descriptor -> qualifier relationship
                self.descriptor_allowed_qualifier.add_relationship(
                    {'sid': descriptor_ui}, {'sid': qualifier_ui}, {'source': 'mesh'}
                )

            #  <ConceptList>
            #    <Concept PreferredConceptYN="Y">
            #     <ConceptUI>M0000001</ConceptUI>
            #     <ConceptName>
            #      <String>Calcimycin</String>
            #     </ConceptName>
            #     <CASN1Name>4-Benzoxazolecarboxylic acid, 5-(methylamino)-2-((3,9,11-trimethyl-8-(1-methyl-2-oxo-2-(1H-pyrrol-2-yl)ethyl)-1,7-dioxaspiro(5.5)undec-2-yl)methyl)-, (6S-(6alpha(2S*,3S*),8beta(R*),9beta,11alpha))-</CASN1Name>
            #     <RegistryNumber>37H9VM9WZL</RegistryNumber>
            #     <ScopeNote>An ionophorous, polyether antibiotic from Streptomyces chartreusensis. It binds and transports CALCIUM and other divalent cations across membranes and uncouples oxidative phosphorylation while inhibiting ATPase of rat liver mitochondria. The substance is used mostly as a biochemical tool to study the role of divalent cations in various biological systems.
            #     </ScopeNote>
            #     <RelatedRegistryNumberList>
            #      <RelatedRegistryNumber>52665-69-7 (Calcimycin)</RelatedRegistryNumber>
            #     </RelatedRegistryNumberList>
            #     <ConceptRelationList>
            #      <ConceptRelation RelationName="NRW">
            #      <Concept1UI>M0000001</Concept1UI>
            #      <Concept2UI>M0353609</Concept2UI>
            #      </ConceptRelation>
            #     </ConceptRelationList>
            #     <TermList>
            #      <Term  ConceptPreferredTermYN="Y"  IsPermutedTermYN="N"  LexicalTag="NON"  RecordPreferredTermYN="Y">
            #       <TermUI>T000002</TermUI>
            #       <String>Calcimycin</String>
            #       <DateCreated>
            #        <Year>1999</Year>
            #        <Month>01</Month>
            #        <Day>01</Day>
            #       </DateCreated>
            #       <ThesaurusIDlist>
            #        <ThesaurusID>FDA SRS (2014)</ThesaurusID>
            #        <ThesaurusID>NLM (1975)</ThesaurusID>
            #       </ThesaurusIDlist>
            #      </Term>
            #     </TermList>
            #    </Concept>

            concepts = descriptor_record.findall('.ConceptList/Concept')

            for concept in concepts:
                preferred_concept = concept.attrib['PreferredConceptYN']

                concept_ui = concept.find('.ConceptUI').text

                # concept node if not exists
                if concept_ui not in check_concepts:
                    concept_properties = {}
                    concept_properties['sid'] = concept_ui
                    concept_properties['name'] = concept.find('.ConceptName/String').text

                    try:
                        concept_properties['scope_note'] = concept.find('.ScopeNote').text
                    except AttributeError as e:
                        pass

                    self.concept.add_node(concept_properties)

                    check_concepts.add(concept_ui)

                # (Descriptor)--(Concept) relation
                self.descriptor_has_concept.add_relationship({'sid': descriptor_ui}, {'sid': concept_ui},
                                                             {'preferred': preferred_concept})

                # concept relations
                for concept_relation in concept.findall('.ConceptRelationList/ConceptRelation'):
                    left = concept_relation.find('.Concept1UI').text
                    right = concept_relation.find('.Concept2UI').text
                    name = concept_relation.attrib['RelationName']

                    self.concept_related_concept.add_relationship({'sid': left}, {'sid': right}, {'name': name})

                # iterate Terms for concept
                for term in concept.findall('.TermList/Term'):
                    term_ui = term.find('TermUI').text
                    concept_preferred_term = term.attrib['ConceptPreferredTermYN']

                    # Term node if not exists
                    if term_ui not in check_terms:
                        term_name = term.find('.String').text
                        self.term.add_node({'sid': term_ui, 'name': term_name})

                        check_terms.add(term_ui)

                    # (Concept)--(Term)
                    self.concept_has_term.add_relationship({'sid': concept_ui}, {'sid': term_ui},
                                                           {'preferred': concept_preferred_term})
