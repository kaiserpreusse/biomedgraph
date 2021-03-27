import logging

from graphpipeline.parser import GffReader
from graphpipeline.parser import ReturnParser
from graphpipeline.datasource import DataSourceVersion
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)


class LncipediaParser(ReturnParser):
    """
    Parse Lncipedia GFF file.

    'lnc_RNA' entries contain gene and transcript IDs as well as mappings to ENSEMBL
    'exon' entries don't have different IDs, they reuse the gene/transcript IDs from their parent 'lnc_RNA' entries

    chr16	lncipedia.org	lnc_RNA	52005479	52026435	.	-	.	ID=lnc-TOX3-1:20;gene_id=lnc-TOX3-1;transcript_id=lnc-TOX3-1:20;gene_alias_1=XLOC_011939;gene_alias_2=linc-SALL1-6;transcript_alias_1=TCONS_00025002;transcript_alias_2=NONHSAT142490;
    chr10	lncipedia.org	exon	8052243	8052735	.	-	.	Parent=GATA3-AS1:5;gene_id=GATA3-AS1;transcript_id=GATA3-AS1:5;gene_alias_1=XLOC_008724;gene_alias_2=linc-KIN-5;gene_alias_3=ENSG00000243350;gene_alias_4=RP11-379F12.3;gene_alias_5=ENSG00000243350.1;gene_alias_6=OTTHUMG00000017641.1;gene_alias_7=ENSG00000197308.9;gene_alias_8=GATA3-AS1;transcript_alias_1=TCONS_00017730;transcript_alias_2=ENST00000458727;transcript_alias_3=ENST00000458727.1;transcript_alias_4=RP11-379F12.3-001;transcript_alias_5=OTTHUMT00000046722.1;transcript_alias_6=NONHSAT011314;transcript_alias_7=NR_104327;transcript_alias_8=NR_104327.1;


    """
    
    def __init__(self):
        super(LncipediaParser, self).__init__()

        self.genes = NodeSet(['Gene'], merge_keys=['sid'])
        self.transcripts = NodeSet(['Transcript'], merge_keys=['sid'])
        self.gene_codes_transcripts = RelationshipSet('CODES', ['Gene'], ['Transcript'], ['sid'], ['sid'])
        self.gene_maps_gene = RelationshipSet('MAPS', ['Gene'], ['Gene'], ['sid'], ['sid'])
        self.transcript_maps_transcript = RelationshipSet('MAPS', ['Transcript'], ['Transcript'], ['sid'], ['sid'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        log.debug(f"Run {self.__class__.__name__}")
        lncipedia_instance = self.get_instance_by_name('Lncipedia')

        lncipedia_datasource_name = lncipedia_instance.datasource.name

        gff_file = lncipedia_instance.get_file('lncipedia_5_2_hg38.gff')

        annotation = GffReader(gff_file)

        check_ids = set()

        for r in annotation.records:
            if r.type == 'lnc_RNA':
                # create gene
                gene_id = r.attributes['gene_id']
                if gene_id not in check_ids:
                    self.genes.add_node({'sid': gene_id, 'source': lncipedia_datasource_name})
                    check_ids.add(gene_id)

                transcript_id = r.attributes['transcript_id']
                if transcript_id not in check_ids:
                    self.transcripts.add_node({'sid': transcript_id, 'source': lncipedia_datasource_name})
                    check_ids.add(transcript_id)

                if frozenset((gene_id, transcript_id)) not in check_ids:
                    self.gene_codes_transcripts.add_relationship(
                        {'sid': gene_id}, {'sid': transcript_id}, {}
                    )
                    check_ids.add(frozenset((gene_id, transcript_id)))

                for k,v in r.attributes.items():
                    if k.startswith('gene_alias'):
                        ref_gene_id = v.split('.')[0]
                        # don't create MAPS relationship if same name like mapped entity
                        if gene_id != ref_gene_id:
                            if frozenset((gene_id, ref_gene_id)) not in check_ids:
                                self.gene_maps_gene.add_relationship(
                                    {'sid': gene_id}, {'sid': ref_gene_id}, {'source': lncipedia_datasource_name}
                                )
                                check_ids.add(frozenset((gene_id, ref_gene_id)))

                    if k.startswith('transcript_alias'):
                        ref_transcript_id = v.split('.')[0]
                        # don't create MAPS relationship if same name like mapped entity
                        if transcript_id != ref_transcript_id:
                            if frozenset((transcript_id, ref_transcript_id)) not in check_ids:
                                self.transcript_maps_transcript.add_relationship(
                                    {'sid': transcript_id}, {'sid': ref_transcript_id}, {'source': lncipedia_datasource_name}
                                )
                                check_ids.add(frozenset((transcript_id, ref_transcript_id)))
