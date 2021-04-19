import gzip
import logging
import os
from uuid import uuid4

from graphio import NodeSet, RelationshipSet

from biomedgraph.datasources.ensembl import Ensembl
from graphpipeline.parser import GffReader
from graphpipeline.parser import ReturnParser

log = logging.getLogger(__name__)


class EnsemblEntityParser(ReturnParser):


    def __init__(self):
        """
        :param ensembl_instance: The ENSEMBL DataSource instance.
        """
        super(EnsemblEntityParser, self).__init__()

        # arguments
        self.arguments = ['taxid']

        # NodeSets
        self.genes = NodeSet(['Gene'], merge_keys=['sid'], default_props={'source': 'ensembl'})
        self.transcripts = NodeSet(['Transcript'], merge_keys=['sid'], default_props={'source': 'ensembl'})
        self.proteins = NodeSet(['Protein'], merge_keys=['sid'], default_props={'source': 'ensembl'})

        # RelationshipSets
        self.gene_codes_transcript = RelationshipSet('CODES', ['Gene'], ['Transcript'], ['sid'], ['sid'], default_props={'source': 'ensembl'})
        self.transcript_codes_protein = RelationshipSet('CODES', ['Transcript'], ['Protein'], ['sid'], ['sid'], default_props={'source': 'ensembl'})

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):
        ensembl_instance = self.get_instance_by_name('Ensembl')
        datasource_name = ensembl_instance.datasource.name

        # try patched path, if not available take flat
        ensembl_gtf_file_path = Ensembl.get_gtf_file_path(taxid, ensembl_instance, patched=True)
        if not os.path.exists(ensembl_gtf_file_path):
            ensembl_gtf_file_path = Ensembl.get_gtf_file_path(taxid, ensembl_instance, patched=False)

        annotation = GffReader(ensembl_gtf_file_path)

        check_gene_ids = set()
        check_transcript_ids = set()
        check_protein_ids = set()
        check_gene_transcript_rels = set()
        check_transcript_protein_rels = set()
        log.info("Start parsing ENSEMBL gtf file, taxid {}, {}".format(taxid, ensembl_gtf_file_path))
        for r in annotation.records:

            # add gene node
            gene_id = r.attributes['gene_id']
            if gene_id not in check_gene_ids:
                props = {'sid': gene_id, 'name': r.attributes['gene_name'], 'taxid': taxid}

                self.genes.add_node(props)
                check_gene_ids.add(gene_id)

            # add transcript node
            if r.type == 'transcript':
                transcript_id = r.attributes['transcript_id']
                if transcript_id not in check_transcript_ids:
                    props = {'sid': transcript_id, 'taxid': taxid}

                    self.transcripts.add_node(props)
                    check_transcript_ids.add(transcript_id)

            # add protein node
            if r.type == 'CDS':
                protein_id = r.attributes['protein_id']
                if protein_id not in check_protein_ids:
                    props = {'sid': protein_id, 'taxid': taxid}

                    self.proteins.add_node(props)
                    check_protein_ids.add(protein_id)

            # Gene-CODES-Transcript
            if r.type == 'transcript':
                transcript_id = r.attributes['transcript_id']
                gene_id = r.attributes['gene_id']

                # add gene-transcript rel
                if gene_id + transcript_id not in check_gene_transcript_rels:
                    self.gene_codes_transcript.add_relationship({'sid': gene_id}, {'sid': transcript_id},
                                                                {})
                    check_gene_transcript_rels.add(gene_id + transcript_id)

            # Transcript-CODES-Protein
            if r.type == 'CDS':
                protein_id = r.attributes['protein_id']
                transcript_id = r.attributes['transcript_id']

                # add transcript-protein rel
                if transcript_id + protein_id not in check_transcript_protein_rels:
                    self.transcript_codes_protein.add_relationship({'sid': transcript_id}, {'sid': protein_id},
                                                                   {})
                    check_transcript_protein_rels.add(transcript_id + protein_id)

        log.info("Finished parsing ENSEMBL gtf file.")


class EnsemblLocusParser(ReturnParser):


    def __init__(self):
        """
        :param ensembl_instance: The ENSEMBL DataSource instance.
        """
        super(EnsemblLocusParser, self).__init__()

        # arguments
        self.arguments = ['taxid']

        # NodeSets
        self.locus = NodeSet(['Locus'], merge_keys=['uuid'], default_props={'source': 'ensembl'})

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):
        ensembl_instance = self.get_instance_by_name('Ensembl')
        datasource_name = ensembl_instance.datasource.name

        ensembl_gtf_file_path = Ensembl.get_gtf_file_path(taxid, ensembl_instance)

        annotation = GffReader(ensembl_gtf_file_path)

        log.info("Start parsing ENSEMBL gtf file, taxid {}, {}".format(taxid, ensembl_gtf_file_path))
        for r in annotation.records:

            # one line is one unique Locus
            props = {'chr': r.chr, 'annotation_source': r.source, 'start': int(r.start), 'end': int(r.end),
                     'type': r.type, 'score': r.score, 'strand': r.strand, 'frame': r.frame,
                     'taxid': taxid, 'ref': 'h38', 'uuid': str(uuid4())}
            props.update(r.attributes)

            self.locus.add_node(props)

        log.info("Finished parsing ENSEMBL gtf file.")


class EnsemblMappingParser(ReturnParser):
    """
    Get mappings from ENSEMBL IDs to other databases.

    ENSEMBL dumps common mapping data to files in the `tsv` directory.


    ### Transcripts
    Extract (Transcript {ensembl})-[MAPS]-(Transcript {refseq}) mappings from ENSEMBL.

    Mappings to NCBI Gene are from: Homo_sapiens.GRCh38.91.refseq.tsv.gz

    Example:
        gene_stable_id|transcript_stable_id|protein_stable_id|xref|db_name|info_type|source_identity|xref_identity|linkage_type
        ENSG00000223972	ENST00000456328	-	102725121	EntrezGene	DEPENDENT	-	-	-


    ### Genes
    Extract (Gene {ensembl})-[MAPS]-(Gene {ncbigene}) mappings from ENSEMBL.

    Mappings to NCBI Gene are from: Homo_sapiens.GRCh38.91.entrez.tsv.gz

    Example:
        gene_stable_id|transcript_stable_id|protein_stable_id|xref|db_name|info_type|source_identity|xref_identity|linkage_type
        ENSG00000223972	ENST00000456328	-	102725121	EntrezGene	DEPENDENT	-	-	-


    ### Proteins
    Extract (Protein {ensembl})-[MAPS]-(Protein {refseq}) mappings from ENSEMBL.

    Mappings to NCBI Gene are from: Homo_sapiens.GRCh38.91.uniprot.tsv.gz

    Example:
        gene_stable_id	transcript_stable_id	protein_stable_id	xref	db_name	info_type	source_identity	xref_identity	linkage_type
        ENSG00000186092	ENST00000335137	ENSP00000334393	Q8NH21	Uniprot/SWISSPROT	DIRECT	100	100	-

    """


    def __init__(self):

        super(EnsemblMappingParser, self).__init__()

        # arguments
        self.arguments = ['taxid']

        # define NodeSet and RelationshipSet
        self.gene_maps_gene = RelationshipSet('MAPS', ['Gene'], ['Gene'], ['sid'], ['sid'], default_props={'source': 'ensembl'})
        self.transcript_maps_transcript = RelationshipSet('MAPS', ['Transcript'], ['Transcript'], ['sid'], ['sid'], default_props={'source': 'ensembl'})
        self.protein_maps_protein = RelationshipSet('MAPS', ['Protein'], ['Protein'], ['sid'], ['sid'], default_props={'source': 'ensembl'})

    # define properties that are used in multiple parsing functions
    @property
    def ensembl_instance(self):
        return self.get_instance_by_name('Ensembl')

    @property
    def datasource_name(self):
        return self.ensembl_instance.datasource.name

    def run_gene(self, taxid):

        ensembl_tsv_entrez_file_path = Ensembl.get_tsv_file_path(taxid, 'entrez', self.ensembl_instance)

        log.debug('Ensembl TSV file path: {}'.format(ensembl_tsv_entrez_file_path))

        check_rels = set()

        with gzip.open(ensembl_tsv_entrez_file_path, 'rt') as f:
            lines = f.readlines()
            for l in lines[1:]:
                flds = l.strip().split()

                ensembl_gene_id = flds[0]
                ncbi_gene_id = flds[3]

                if frozenset([ensembl_gene_id, ncbi_gene_id]) not in check_rels:
                    self.gene_maps_gene.add_relationship({'sid': ensembl_gene_id}, {'sid': ncbi_gene_id},
                                                         {})
                    check_rels.add(frozenset([ensembl_gene_id, ncbi_gene_id]))

    def run_transcript(self, taxid):

        ensembl_tsv_refseq_file_path = Ensembl.get_tsv_file_path(taxid, 'refseq', self.ensembl_instance)
        log.debug('Ensembl TSV file path: {}'.format(ensembl_tsv_refseq_file_path))
        check_rels = set()

        with gzip.open(ensembl_tsv_refseq_file_path, 'rt') as f:
            lines = f.readlines()
            for l in lines[1:]:
                flds = l.strip().split()

                ensembl_transcript_id = flds[1]
                xref_id = flds[3]

                # filter transcripts
                second_letter = xref_id[1]

                if second_letter == 'M' or second_letter == 'R':
                    if frozenset([ensembl_transcript_id, xref_id]) not in check_rels:
                        self.transcript_maps_transcript.add_relationship(
                            {'sid': ensembl_transcript_id},
                            {'sid': xref_id},
                            {}
                        )

                        check_rels.add(frozenset([ensembl_transcript_id, xref_id]))

    def run_protein(self, taxid):
        ensembl_tsv_uniprot_file_path = Ensembl.get_tsv_file_path(taxid, 'uniprot', self.ensembl_instance)
        log.debug('Ensembl TSV file path: {}'.format(ensembl_tsv_uniprot_file_path))

        check_rels = set()
        with gzip.open(ensembl_tsv_uniprot_file_path, 'rt') as f:
            lines = f.readlines()
            for l in lines[1:]:
                flds = l.strip().split()

                ensembl_protein_id = flds[2]
                xref_id = flds[3]

                if frozenset([ensembl_protein_id, xref_id]) not in check_rels:
                    self.protein_maps_protein.add_relationship(
                        {'sid': ensembl_protein_id}, {'sid': xref_id},
                        {'taxid': self.taxid}
                    )

                    check_rels.add(frozenset([ensembl_protein_id, xref_id]))

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):
        self.run_gene(taxid)
        self.run_transcript(taxid)
        self.run_protein(taxid)
#
#
# class Ensembl_Locus_Parser(Parser):
#     """
#     Extract Loci from ENSEMBL gtf file.
#
#     e.g. /.../Ensembl/92/gtf/homo_sapiens/Homo_sapiens.GRCh38.92.gtf.gz
#
#     """
#
#     DESCRIPTION = ParserDescription('ensembl', Entity.TYPE, labels=['Locus'], data_grouping=['taxid'])
#
#     def __init__(self, ensembl_gtf_file, taxid):
#         super(Ensembl_Locus_Parser, self).__init__()
#
#         self.ensembl_gtf_file = ensembl_gtf_file
#         self.taxid = taxid
#
#     def run(self):
#         annotation = GffReader(self.ensembl_gtf_file)
#
#         for r in annotation.records:
#             props = {'chr': r.chr, 'annotation_source': r.source, 'start': int(r.start), 'end': int(r.end),
#                      'type': r.type, 'score': r.score, 'strand': r.strand, 'frame': r.frame,
#                      'source': self.DESCRIPTION.datasource, 'taxid': self.taxid, 'ref': 'h38'}
#             props.update(r.attributes)
#
#             yield Entity(self.DESCRIPTION.labels, props)
#
#
# class Ensembl_Gene_Is_Locus_Parser(Parser):
#     """
#     Extract ENSEMBL (Gene)-[IS]-(Locus) relationships from gtf file.
#
#
#     The combination of chr, start, end, type is NOT UNIQUE.
#
#     Question: Which line in the GTF file should be matche wo which gene/transcript?
#
#     ALL lines contain gene_id in their attributes.
#     MOST lines contain transcript_id in their attributes.
#
#     Ideas:
#         - match only Locus with type gene to Gene
#         - match everything based on gene_id and transcript_id
#
#     Run on database and do not parse from here:
#
#     MATCH (g:Gene), (l:Locus)
#     WHERE g.sid = l.gene_id
#     CREATE (g)-[:ID]->(l)
#
#
#     # differentiating factor of gene seems to be strand
#
#     `MATCH (n:Locus) WHERE n.chr = '12' AND n.start = 64622509 AND n.end = 64622605 AND n.type = 'gene' RETURN n;`
#
#     this query shows that it is unique with strand:
#
#     `MATCH (n:Locus) WHERE n.type = 'gene' RETURN DISTINCT n.chr, n.start, n.end, n.type, n.strand,
#     count(n) ORDER BY count(n) DESC LIMIT 10;`
#
#     # unclear why so many exons with same coordinates
#
#     `MATCH (n:Locus) WHERE n.chr = '4' AND n.start = 86354530 AND n.end = 86354644 AND n.type = 'exon' RETURN n;`
#
#     this example has many transcript_ids but only one gene_id. the match becomes unique with transcript_id:
#
#     `MATCH (n:Locus) WHERE n.type = 'exon' RETURN DISTINCT n.chr, n.start, n.end, n.type, n.transcript_id,
#     count(n) ORDER BY count(n) DESC LIMIT 10;`
#     """
#
#     DESCRIPTION = ParserDescription('ensembl', Relationship.TYPE, data_grouping=['taxid'],
#                                     start_node_labels=['Gene'], end_node_labels=['Locus'],
#                                     relationship_type='IS')
#
#     def __init__(self, ensembl_gtf_file, taxid):
#         super(Ensembl_Gene_Is_Locus_Parser, self).__init__()
#         self.ensembl_gtf_file = ensembl_gtf_file
#         self.taxid = taxid
#
#     def run(self):
#
#         annotation = GffReader(self.ensembl_gtf_file)
#
#         for r in annotation.records:
#             if r.type == 'gene':
#                 pass
#                 # yield Relationship(self.DESCRIPTION.relationship_type, self.DESCRIPTION.start_node_labels,
#                 #                  self.DESCRIPTION.end_node_labels, {'sid': r.attributes['gene_id']},
#                 #                  {'start': r.start, 'end': r.end, 'chr': r.chr, 'type': 'gene'},
#                 #                  {'source': self.DESCRIPTION.datasource, 'taxid': self.taxid})
