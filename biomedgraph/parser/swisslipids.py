import gzip
import logging

from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)


class SwissLipidsParser(ReturnParser):

    def __init__(self, root_dir):

        super(SwissLipidsParser, self).__init__(root_dir)

        # define NodeSet and RelationshipSet
        self.lipids = NodeSet(['Lipid'], merge_keys=['sid'])

        self.lipid_fromclass_lipid = RelationshipSet('FROM_LIPID_CLASS', ['Lipid'], ['Lipid'], ['sid'], ['sid'])
        self.lipid_parent_lipid = RelationshipSet('HAS_PARENT', ['Lipid'], ['Lipid'], ['sid'], ['sid'])
        self.lipid_component_lipid = RelationshipSet('HAS_COMPONENT', ['Lipid'], ['Lipid'], ['sid'], ['sid'])
        self.lipid_maps_metabolite = RelationshipSet('MAPS', ['Lipid'], ['Metabolite'], ['sid'], ['sid'])
        self.lipid_associates_protein = RelationshipSet('HAS_ASSOCIATION', ['Lipid'], ['Protein'], ['sid'], ['sid'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):

        swisslipids_instance = self.get_instance_by_name('SwissLipids')

        self.get_lipids(swisslipids_instance)
        self.get_lipid_to_protein(swisslipids_instance)

    def get_lipids(self, instance):
        """
        Lipid ID	Level	Name	Abbreviation*	Synonyms*	Lipid class*	Parent	Components*	SMILES (pH7.3)	InChI (pH7.3)	InChI key (pH7.3)	Formula (pH7.3)	Charge (pH7.3)	Mass (pH7.3)	Exact Mass (neutral form)	Exact m/z of [M.]+	Exact m/z of [M+H]+	Exact m/z of [M+K]+ 	Exact m/z of [M+Na]+	Exact m/z of [M+Li]+	Exact m/z of [M+NH4]+	Exact m/z of [M-H]-	Exact m/z of [M+Cl]-	Exact m/z of [M+OAc]- 	CHEBI	LIPID MAPS	HMDB	PMID
        SLM:000000002	Class	Ceramide (iso-d17:1(4E))	Cer(iso-d17:1(4E))	N-acyl-15-methylhexadecasphing-4-enine	SLM:000399814			CC(C)CCCCCCCCC\C=C\[C@@H](O)[C@H](CO)NC([*])=O	InChI=none		C18H34NO3R	0												70846			14685263 | 21325339 | 9603947 | 21926990
        SLM:000000003	Isomeric subspecies	15-methylhexadecasphing-4-enine			SLM:000390097			CC(C)CCCCCCCCC\C=C\[C@@H](O)[C@@H]([NH3+])CO	InChI=1S/C17H35NO2/c1-15(2)12-10-8-6-4-3-5-7-9-11-13-17(20)16(18)14-19/h11,13,15-17,19-20H,3-10,12,14,18H2,1-2H3/p+1/b13-11+/t16-,17+/m0/s1	InChIKey=LZKPPSAEINBHRP-KORIGIIASA-O	C17H36NO2	1	286.473200	285.266779	285.266231	286.274056	324.229938	308.256000	292.282235	303.300605	284.259503	320.236181	344.280632	70771			19372430

        Columns:
            - difficult to select column names, use index
            - the * means the field is a list
            - different field separators in list fields

        0	Lipid ID
        1	Level
        2	Name
        3	Abbreviation*
        4	Synonyms*
        5	Lipid class*
        6	Parent
        7	Components*
        8	SMILES (pH7.3)
        9	InChI (pH7.3)
        10	InChI key (pH7.3)
        11	Formula (pH7.3)
        12	Charge (pH7.3)
        13	Mass (pH7.3)
        14	Exact Mass (neutral form)
        15	Exact m/z of [M.]+
        16	Exact m/z of [M+H]+
        17	Exact m/z of [M+K]+
        18	Exact m/z of [M+Na]+
        19	Exact m/z of [M+Li]+
        20	Exact m/z of [M+NH4]+
        21	Exact m/z of [M-H]-
        22	Exact m/z of [M+Cl]-
        23	Exact m/z of [M+OAc]-
        24	CHEBI
        25	LIPID MAPS
        26	HMDB
        27	PMID
        """

        lipids_file = instance.get_file('lipids.tsv.gz')

        # get header
        header = None

        with gzip.open(lipids_file, 'rt') as f:
            header = next(f)
        header = header.strip().split('\t')

        def safe_string(s):
            for char in [' ', '[', ']', '(', ')', '*', '/']:
                s = s.replace(char, '_')
            return s

        header_cypher_safe = [safe_string(s) for s in header]
        log.debug(header_cypher_safe)

        # iterate file
        with gzip.open(lipids_file, 'rt', errors="replace") as f:
            # skip header
            next(f)

            for l in f:
                flds = l.strip().split('\t')


                lipid_sid = flds[0]

                # (Lipid) node
                props = {'source': 'swisslipids'}
                props['sid'] = lipid_sid

                # add all properties, some are empty but contain whitespaces
                for i, fld in enumerate(flds):
                    fld = fld.strip()
                    if fld:
                        props[header_cypher_safe[i]] = fld
                #
                # print(
                #     dict(zip(header, flds))
                # )

                self.lipids.add_node(props)

                # (Lipid)-[FROM_LIPID_CLASS]-(Lipid)
                for lipid_class_sid in flds[5].strip().split('|'):
                    # strip leading/trailing spaces, not always existing
                    lipid_class_sid = lipid_class_sid.strip()
                    self.lipid_fromclass_lipid.add_relationship(
                        {'sid': lipid_sid}, {'sid': lipid_class_sid}, {'source': 'swisslipids'}
                    )

                # (Lipid)-[HAS_PARENT]-(Lipid)
                self.lipid_parent_lipid.add_relationship(
                    {'sid': lipid_sid}, {'sid': flds[6].strip()}, {'source': 'swisslipids'}
                )

                # (Lipid)-[COMPONENT]-(Lipid)
                ## e.g. SLM:000000510 (sn1) / SLM:000000418 (sn2)
                for lipid_component in flds[7].strip().split('/'):
                    # get sid and type of lipid component, type does not always exist
                    try:
                        lipid_component_sid, lipid_component_type = lipid_component.strip().split(' ', 1)

                        self.lipid_component_lipid.add_relationship(
                            {'sid': lipid_sid}, {'sid': lipid_component_sid}, {'type': lipid_component_type}
                        )
                    # some empty fields contain extra spaces
                    except ValueError:
                        pass

                # (Lipid)-[MAPS]-(Metabolite)
                try:
                    chebi_id = flds[24].strip()

                    if chebi_id:
                        self.lipid_maps_metabolite.add_relationship(
                            {'sid': lipid_sid}, {'sid': chebi_id}, {'source': 'swisslipids'}
                        )
                except IndexError:
                    pass

                try:
                    hmdb_id = flds[26].strip()

                    if hmdb_id:

                        self.lipid_maps_metabolite.add_relationship(
                            {'sid': lipid_sid}, {'sid': hmdb_id}, {'source': 'swisslipids'}
                        )
                except IndexError:
                    pass

    def get_lipid_to_protein(self, instance):
        """
        File: lipids2uniprot.tsv.gz

        Columns:
            - difficult to select column names, use index


        0	metabolite id
        1	UniprotKB IDs
        2	level
        3	metabolite name
        4	abbreviations
        5	synonyms
        6	lipid class
        7	components
        8	PMIDs
        9	SMILES (pH7.3)
        10	InChI (pH7.3)
        11	InChI key (pH7.3)
        12	Formula (pH7.3)
        13	Mass (pH7.3)
        14	Charge (pH7.3)
        15	Exact Mass (neutral form)
        16	Exact m/z of [M.]+
        17	Exact m/z of [M+H]+
        18	Exact m/z of [M+K]+Exact m/z of [M+Na]+
        19	Exact m/z of [M+Li]+
        20	Exact m/z of [M+NH4]+
        21	Exact m/z of [M-H]-
        22	Exact m/z of [M+Cl]-
        23	Exact m/z of [M+OAc]-
        24	ChEBI
        25	LipidMaps
        26	HMDB
        27	Mapping level

        :param instance: The datasource instance.
        """
        lipids_2_protein_file = instance.get_file('lipids2uniprot.tsv.gz')


        # iterate file
        with gzip.open(lipids_2_protein_file, 'rt', errors="replace") as f:
            next(f)
            for l in f:
                flds = l.strip().split('\t')
                swisslipids_id = flds[0].strip()
                mapping_level = flds[27].strip()

                # collect UniProt IDs from uniprot fields, contains a '|' separated list
                # G5EC84 | O18037 | P91079 | Q09517 | Q10916 | Q20735 | Q21054 | Q23498 | Q9U3D4
                # note: not always formatted with space: ' | '
                uniprot_id_string = flds[1]
                uniprot_ids = set()
                for u in uniprot_id_string.split('|'):
                    u = u.strip()
                    if u:
                        uniprot_ids.add(u)

                for up in uniprot_ids:
                    self.lipid_associates_protein.add_relationship(
                        {'sid': swisslipids_id}, {'sid': up}, {'source': 'swisslipids', 'level': mapping_level}
                    )
