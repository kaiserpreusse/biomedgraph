from itertools import combinations

from elderberry import ReturnParser
from graphio import RelationshipSet


class NcbiHomoloGeneParser(ReturnParser):
    """
    The NCBI HomoloGene parser reads the basic datafile `homologene.data` from HomoloGene.

    The file `homolgene.data` is a tab separated list of homology groups.

    Fields: group ID, tax ID, gene ID, gene symbol, unclear?, refseq ID

    Example::

    3	9606	34	ACADM	4557231	NP_000007.1
    3	9598	469356	ACADM	160961497	NP_001104286.1
    3	9544	705168	ACADM	109008502	XP_001101274.1
    3	9615	490207	ACADM	545503811	XP_005622188.1

    """

    def __init__(self, root_dir):
        super(NcbiHomoloGeneParser, self).__init__(root_dir)

        # output data
        self.gene_homolog_gene = RelationshipSet('HOMOLOG', ['Gene'], ['Gene'],
                                                 ['sid'], ['sid'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        ncbihomologene_instance = self.get_instance_by_name('NcbiHomoloGene')
        datafile = ncbihomologene_instance.get_file('homologene.data')

        with open(datafile) as f:

            current_group_id = None
            current_group_genes = set()

            for l in f:
                # iterate and collect groups (identified by forst column)
                # take gene IDs from group and create all pairwise relationships
                flds = l.strip().split('\t')
                group_id = flds[0]
                gene_id = flds[2]

                # set group_id on first line
                if not current_group_id:
                    current_group_id = group_id

                if current_group_id == group_id:
                    current_group_genes.add(gene_id)

                else:
                    # first line with new group_id
                    # create relationships for all gene_id from previous group
                    for g1, g2 in combinations(current_group_genes, 2):
                        self.gene_homolog_gene.add_relationship(
                            {'sid': g1}, {'sid': g2}, {}
                        )

                    # clear gene set
                    current_group_genes = set()
                    # add current gene_id which is the first from a new group
                    current_group_genes.add(gene_id)
                    # set current_group_id to this group_id
                    current_group_id = group_id
