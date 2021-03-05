from graphpipeline.parser import ReturnParser
from graphio import NodeSet, RelationshipSet


class HGNCParser(ReturnParser):


    def __init__(self):
        """

        :param ncbigene_instance: NcbiGene Instance
        :type ncbigene_instance: DataSourceInstance
        :param taxid:
        """
        super(HGNCParser, self).__init__()

        # output data
        self.genes = NodeSet(['Gene'], merge_keys=['sid'])

        self.gene_maps_gene = RelationshipSet('MAPS', ['Gene'], ['Gene'],
                                              ['sid'], ['sid'])
        self.gene_maps_genesymbol = RelationshipSet('MAPS', ['Gene'], ['GeneSymbol'], ['sid'], ['sid', 'taxid'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):

        hgnc_instance = self.get_instance_by_name('HGNC')

        hgnc_complete_file = hgnc_instance.get_file('hgnc_complete_set.txt')
        self.parse_hgnc_complete_file(hgnc_complete_file)

    def parse_hgnc_complete_file(self, hgnc_complete_file):
        with open(hgnc_complete_file, 'rt') as f:
            header = next(f)

            for l in f:
                flds = l.strip().split('\t')
                sid = flds[0]
                gene_symbol = flds[1]
                ncbi_id = flds[18] if len(flds) > 18 else None
                ensembl_id = flds[19] if len(flds) > 19 else None

                all_props = dict(zip(header, flds))
                all_props['sid'] = sid
                all_props['source'] = 'hgnc'

                self.genes.add_node(all_props)

                if ncbi_id:
                    self.gene_maps_gene.add_relationship({'sid': sid}, {'sid': ncbi_id}, {'source': 'hgnc'})
                if ensembl_id:
                    self.gene_maps_gene.add_relationship({'sid': sid}, {'sid': ensembl_id}, {'source': 'hgnc'})

                if gene_symbol:
                    self.gene_maps_genesymbol.add_relationship({'sid': sid}, {'sid': gene_symbol, 'taxid': '9606'}, {'source': 'hgnc'})
