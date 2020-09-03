import pandas

from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet


class MirbaseParser(ReturnParser):

    def __init__(self, root_dir):
        super(MirbaseParser, self).__init__(root_dir)

        # NodeSets
        self.precursor_mirna = NodeSet(['PrecursorMirna'], merge_keys=['sid'])
        self.mature_mirna = NodeSet(['Mirna'], merge_keys=['sid'])
        # RelationshipSets
        self.precursor_codes_mature = RelationshipSet('PRE', ['PrecursorMirna'], ['Mirna'], ['sid'], ['sid'])
        self.transcript_codes_precursor = RelationshipSet('IS', ['Transcript'], ['PrecursorMirna'], ['sid'], ['sid'])
        self.gene_is_precursor = RelationshipSet('IS', ['Gene'], ['PrecursorMirna'], ['sid'], ['sid'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        self.get_mature_mirnas()
        self.get_pre_mirnas()
        self.get_pre_mature_relationship()
        self.get_pre_transcript_relationships()
        self.get_gene_pre_relationships()

    @property
    def mirbase_instance(self):
        return self.get_instance_by_name('Mirbase')

    @property
    def pre_mirna_df(self):
        """
        Ge precursor miRNA DataFrame from  mirna.txt.gz

        mir_acc, mir_id, prev_mir_id, desc, sequence, comment, organism_key, dead_flag
        """
        precursor_mirna_table_file = self.mirbase_instance.get_file('mirna.txt.gz')
        pre_mirs_df = pandas.read_csv(precursor_mirna_table_file, sep='\t', index_col=0, header=None)
        pre_mirs_df.columns = ['mir_acc', 'mir_id', 'prev_mir_id', 'desc', 'sequence', 'comment', 'organism_key',
                               'dead_flag']
        return pre_mirs_df

    @property
    def mature_mirna_df(self):
        """
        Get mature miRNA DataFrame from: mirna_mature.txt.gz

        name, prev_name, mir_acc, evidence, ref, similarity, dead_flag
        """
        mirna_table_file = self.mirbase_instance.get_file('mirna_mature.txt.gz')
        mirnas_df = pandas.read_csv(mirna_table_file, sep='\t', index_col=0, header=None)
        mirnas_df.columns = ['name', 'prev_name', 'mir_acc', 'evidence', 'ref', 'similarity', 'dead_flag']
        return mirnas_df

    @property
    def context_df(self):
        """
        Get context DataFrame from: mirna_context.txt.gz

        auto_mirna  transcript_id   overlap_sense   overlap_type    number  transcript_source   transcript_name
        64777	    ENST00000545242	+	            intron	        15	    HGNC_trans_name	    ABLIM2-203
        """
        mirna_context_file = self.mirbase_instance.get_file('mirna_context.txt.gz')
        mirna_context_df = pandas.read_csv(mirna_context_file, sep='\t', index_col=0, header=None)
        mirna_context_df.columns = ['transcript_id', 'overlap_sense', 'overlap_type', 'number', 'transcript_source',
                                    'transcript_name']
        return mirna_context_df

    @property
    def mirna_database_url_df(self):
        """
        Database list from: mirna_database_url.txt.gz

        `auto_db`, `display_name`, `url`        
        5	EntrezGene	https://www.ncbi.nlm.nih.gov/gene/<?>
        """
        file = self.mirbase_instance.get_file('mirna_database_url.txt.gz')
        df = pandas.read_csv(file, sep='\t', index_col=0, header=None)
        df.columns = ['display_name', 'url']
        return df

    @property
    def mirna_database_link_df(self):
        """
        Database links from: mirna_database_links.txt.gz

        'auto_mirna', 'auto_db', 'link', 'display_name'
        64744	5	406883	MIRLET7A3
        """
        file = self.mirbase_instance.get_file('mirna_database_links.txt.gz')
        df = pandas.read_csv(file, sep='\t', index_col=0, header=None)
        df.columns = ['auto_db', 'link', 'display_name']
        return df

    def get_mature_mirnas(self):
        """
        Mature miRNAs are stored in a single table: mirna_mature.txt.gz

        name, prev_name, mir_acc, evidence, ref, similarity, dead_flag
        """
        for row in self.mature_mirna_df.itertuples():
            # add node
            self.mature_mirna.add_node({'sid': row.mir_acc, 'name': row.name, 'evidence': row.evidence})

    def get_pre_mirnas(self):
        """
        Precursor miRNAs are stored in a table: mirna.txt.gz

        mir_acc, mir_id, prev_mir_id, desc, sequence, comment, organism_key

        Organism identifier for the precursor miRNAs are in another table: mirna_species.txt.gz

        organism, division, org_name, taxonomy, genome_assembly, genome_accession, ensembl_db

        `org_name` is the long name of the organism, there is no taxonomy ID.

        :return: List of precursor miRNAs
        :rtype: list[Entity]
        """

        organism_table_file = self.mirbase_instance.get_file('mirna_species.txt.gz')

        # load pre-miRNA table

        # load organism table
        orgs_df = pandas.read_csv(organism_table_file, sep='\t', index_col=0, header=None)
        orgs_df.columns = ['organism', 'division', 'org_name', 'taxon_id', 'taxonomy', 'genome_assembly',
                           'genome_accession',
                           'ensembl_db']

        merged_pre_mirs_org_df = pandas.merge(self.pre_mirna_df, orgs_df, on=None, left_on='organism_key',
                                              right_index=True)

        # add precursor miRNA nodes
        for row in merged_pre_mirs_org_df.itertuples():
            props = {'sid': row.mir_acc, 'name': row.mir_id, 'desc': row.desc, 'sequence': row.sequence,
                     'taxid': row.taxon_id, 'comment': str(row.comment)}

            self.precursor_mirna.add_node(props)

    def get_pre_mature_relationship(self):
        """
        Mature miRNAs and precursor miRNAs are in the same files described in the respective parser function (above).

        Mapping is stored in a mapping table: mirna_pre_mature.txt.gz

        pre_dbid, mature_dbid, start, end

        It contains the primary key of mature and precursor miRNA tables and the start/end of the mature
        sequence within the precursor.

        :return: List of relationships between mature and precursor miRNAs
        :rtype: list[Relationship]
        """
        mapping_table_file = self.mirbase_instance.get_file('mirna_pre_mature.txt.gz')

        # collect db_primary_key -> mirBase accession to later parse
        # the mature/precursor mapping table

        precursor_db_key_2_accession = {}

        for row in self.pre_mirna_df.itertuples():
            precursor_db_key_2_accession[row.Index] = row.mir_acc

        # mature miRNAs

        # get miRNAs from miRNAs table first
        # they are mapped to pre-miRNAs with a mapping table that contains the position of the mature sequence
        # organims is also only stored for pre-miRNA

        # collect db_primary_key -> mirBase accession to later parse
        # the mature/precursor mapping table
        mature_db_key_2_accession = {}

        for row in self.mature_mirna_df.itertuples():
            mature_db_key_2_accession[row.Index] = row.mir_acc

        # parse mappings

        # get mapping table
        pre_2_mature_df = pandas.read_csv(mapping_table_file, sep='\t', index_col=False, header=None)
        pre_2_mature_df.columns = ['pre_dbid', 'mature_dbid', 'start', 'end']

        # iterate over mapping table and create relationships

        for row in pre_2_mature_df.itertuples():
            mature_acc = mature_db_key_2_accession[row.mature_dbid]
            precursor_acc = precursor_db_key_2_accession[row.pre_dbid]

            self.precursor_codes_mature.add_relationship(
                {'sid': precursor_acc}, {'sid': mature_acc},
                {'start': int(row.start), 'end': int(row.end)}
            )

    def get_pre_transcript_relationships(self):
        """
        MirBase provides the transcriptional context based on ENSEMBL transcipts.

        Context is stored in a single file: mirna_context.txt.gz

        auto_mirna  transcript_id   overlap_sense   overlap_type    number  transcript_source   transcript_name
        64777	    ENST00000545242	+	            intron	        15	    HGNC_trans_name	    ABLIM2-203

        For mapping the auto_mirna KEY we need the  precursor miRNAs from: mirna.txt.gz

        """
        pre_2_context = pandas.merge(self.context_df, self.pre_mirna_df, how='left', on=None, left_index=True,
                                     right_index=True)

        for row in pre_2_context.itertuples():
            self.transcript_codes_precursor.add_relationship(
                {'sid': row.transcript_id}, {'sid': row.mir_acc},
                {'overlap_type': row.overlap_type, 'number': row.number}
            )

    def get_gene_pre_relationships(self):
        """
        Parse relationships from Gene to Mirna.

        MiRBase provides links to external databases in a table:mirna_database_links.txt.gz

        `auto_mirna`, `auto_db`, `link`, `display_name`
        64743	5	406882	MIRLET7A2

        auto_mirna is the mirna KEY from: mirna.txt.gz

        auto_db is the DB KEY from: mirna_database_url.txt.gz


        5	EntrezGene	https://www.ncbi.nlm.nih.gov/gene/<?>

        Example line: 64743	ENTREZGENE		406882	MIRLET7A2
        :return:
        """
        gene_pre_df = pandas.merge(self.mirna_database_link_df, self.mirna_database_url_df, how='left',
                                   left_on='auto_db', right_index=True)
        print(len(self.mirna_database_link_df), len(self.mirna_database_url_df))
        print(len(gene_pre_df))

        final_merge = pandas.merge(gene_pre_df, self.pre_mirna_df, how='left', on=None, left_index=True,
                                   right_index=True)

        for row in final_merge.itertuples():
            if row.display_name_y == 'EntrezGene':
                self.gene_is_precursor.add_relationship(
                    {'sid': row.link}, {'sid': row.mir_acc}, {'source': self.mirbase_instance.datasource.name}
                )

#
#
# def get_pre_gene_relationship(precursor_mirna_table_file, database_links_file):
#     """
#     Parse relationships from Gene to Mirna.
#
#     MiRBase provides links to external databases in a table: mirna_database_links.txt
#
#         db_id, comment, db_link, db_secondary, other_params
#
#     db_id is the primary key of the precursor miRNA table.
#
#     Example line: 64743	ENTREZGENE		406882	MIRLET7A2
#
#     :param precursor_mirna_table_file: Precursor miRNA table file.
#     :param database_links_file: Database links table file.
#     :return: List of Relationships.
#     :rtype: list[Relationships]
#     """
#     output = []
#
#     # load pre-miRNA table
#     pre_mirs_df = pandas.read_csv(precursor_mirna_table_file, sep='\t', index_col=0, header=None)
#     pre_mirs_df.columns = ['mir_acc', 'mir_id', 'prev_mir_id', 'desc', 'sequence', 'comment', 'organism_key']
#
#     # collect db_primary_key -> mirBase accession to later parse
#     # the mature/precursor mapping table
#
#     precursor_db_key_2_accession = {}
#
#     for row in pre_mirs_df.itertuples():
#         precursor_db_key_2_accession[row.Index] = row.mir_acc
#
#     database_links = pandas.read_csv(database_links_file, sep='\t', index_col=0, header=None)
#     database_links.columns = ['db_id', 'comment', 'db_link', 'db_secondary', 'other_params']
#
#     for row in database_links.itertuples():
#         if row.db_id == 'ENTREZGENE':
#             pre_mir_accession = precursor_db_key_2_accession[row.Index]
#
#             ncbi_gene_id = row.db_link
#
#             output.append(
#                 Relationship('IS', ['Gene'], ['PreMirna'], {'sid': ncbi_gene_id}, {'sid': pre_mir_accession},
#                              {'source': 'mirbase'})
#             )
#
#     return output
