import logging
import pandas
import gzip
from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)


class GtexMetadataParser(ReturnParser):


    def __init__(self, root_dir):
        """

        :param mesh_instance: NcbiGene Instance
        :type mesh_instance: DataSourceInstance
        """
        super(GtexMetadataParser, self).__init__(root_dir)

        # NodeSets
        self.tissues = NodeSet(['GtexTissue'], merge_keys=['name'])
        self.detailed_tissues = NodeSet(['GtexDetailedTissue'], merge_keys=['name'])
        self.sample = NodeSet(['GtexSample'], merge_keys=['sid'])

        self.sample_measures_tissue = RelationshipSet('MEASURES', ['GtexSample'], ['GtexTissue'], ['sid'], ['name'])
        self.sample_measures_detailed_tissue = RelationshipSet('MEASURES', ['GtexSample'], ['GtexDetailedTissue'],
                                                               ['sid'], ['name'])
        self.tissue_parent_detailed_tissue = RelationshipSet('PARENT', ['GtexTissue'], ['GtexDetailedTissue'], ['name'],
                                                             ['name'])
        self.tissue_parent_detailed_tissue.unique = True

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        gtex_instance = self.get_instance_by_name('Gtex')

        gtext_sample_attribute_file = gtex_instance.get_file('GTEx_Analysis_v8_Annotations_SampleAttributesDS.txt')

        gtex_df = pandas.read_csv(gtext_sample_attribute_file, sep='\t', header=0, index_col=False,
                                  encoding="utf-8-sig")

        for row in gtex_df.itertuples():
            sid = row.SAMPID
            tissue_name = row.SMTS
            detailed_tissue_name = row.SMTSD

            props = {'sid': sid, 'SMATSSCR': row.SMATSSCR, 'SMCENTER': row.SMCENTER, 'SMPTHNTS': row.SMPTHNTS,
                     'SMRIN': row.SMRIN, 'SMTS': row.SMTS, 'SMTSD': row.SMTSD, 'SMUBRID': row.SMUBRID,
                     'SMTSISCH': row.SMTSISCH, 'SMTSPAX': row.SMTSPAX, 'SMNABTCH': row.SMNABTCH,
                     'SMNABTCHT': row.SMNABTCHT, 'SMNABTCHD': row.SMNABTCHD, 'SMGEBTCH': row.SMGEBTCH,
                     'SMGEBTCHD': row.SMGEBTCHD, 'SMGEBTCHT': row.SMGEBTCHT, 'SMAFRZE': row.SMAFRZE, 'SMGTC': row.SMGTC,
                     'SME2MPRT': row.SME2MPRT, 'SMCHMPRS': row.SMCHMPRS, 'SMNTRART': row.SMNTRART,
                     'SMNUMGPS': row.SMNUMGPS, 'SMMAPRT': row.SMMAPRT, 'SMEXNCRT': row.SMEXNCRT,
                     'SM550NRM': row.SM550NRM, 'SMGNSDTC': row.SMGNSDTC, 'SMUNMPRT': row.SMUNMPRT,
                     'SM350NRM': row.SM350NRM, 'SMRDLGTH': row.SMRDLGTH, 'SMMNCPB': row.SMMNCPB,
                     'SME1MMRT': row.SME1MMRT, 'SMSFLGTH': row.SMSFLGTH, 'SMESTLBS': row.SMESTLBS, 'SMMPPD': row.SMMPPD,
                     'SMNTERRT': row.SMNTERRT, 'SMRRNANM': row.SMRRNANM, 'SMRDTTL': row.SMRDTTL, 'SMVQCFL': row.SMVQCFL,
                     'SMMNCV': row.SMMNCV, 'SMTRSCPT': row.SMTRSCPT, 'SMMPPDPR': row.SMMPPDPR, 'SMCGLGTH': row.SMCGLGTH,
                     'SMGAPPCT': row.SMGAPPCT, 'SMUNPDRD': row.SMUNPDRD, 'SMNTRNRT': row.SMNTRNRT,
                     'SMMPUNRT': row.SMMPUNRT, 'SMEXPEFF': row.SMEXPEFF, 'SMMPPDUN': row.SMMPPDUN,
                     'SME2MMRT': row.SME2MMRT, 'SME2ANTI': row.SME2ANTI, 'SMALTALG': row.SMALTALG,
                     'SME2SNSE': row.SME2SNSE, 'SMMFLGTH': row.SMMFLGTH, 'SME1ANTI': row.SME1ANTI,
                     'SMSPLTRD': row.SMSPLTRD, 'SMBSMMRT': row.SMBSMMRT, 'SME1SNSE': row.SME1SNSE,
                     'SME1PCTS': row.SME1PCTS, 'SMRRNART': row.SMRRNART, 'SME1MPRT': row.SME1MPRT,
                     'SMNUM5CD': row.SMNUM5CD, 'SMDPMPRT': row.SMDPMPRT, 'SME2PCTS': row.SME2PCTS}

            self.sample.add_node(props)
            self.tissues.add_unique({'name': tissue_name})
            self.detailed_tissues.add_unique({'name': detailed_tissue_name})

            self.sample_measures_tissue.add_relationship({'sid': sid}, {'name': tissue_name}, {})
            self.sample_measures_detailed_tissue.add_relationship({'sid': sid}, {'name': detailed_tissue_name}, {})

            self.tissue_parent_detailed_tissue.add_relationship({'name': tissue_name}, {'name': detailed_tissue_name},
                                                                {})


class GtexDataParser(ReturnParser):


    def __init__(self, root_dir):
        """

        :param mesh_instance: NcbiGene Instance
        :type mesh_instance: DataSourceInstance
        """
        super(GtexDataParser, self).__init__(root_dir)

        self.gene_expressed_tissue = RelationshipSet('EXPRESSED', ['Gene'], ['GtexDetailedTissue'], ['sid'], ['name'])

        self.object_sets = [self.gene_expressed_tissue]

        self.container.add_all(self.object_sets)

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        gtex_instance = self.get_instance_by_name('Gtex')

        gtex_mean_gene = gtex_instance.get_file('GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_median_tpm.gct.gz')

        with gzip.open(gtex_mean_gene, 'rt') as f:
            lines = f.readlines()
            # remove first two lines
            lines = lines[2:]
            # get header line
            header = lines.pop(0)
            header_fields = header.split('\t')

            # iterate data lines
            for line in lines:
                flds = line.split('\t')
                gene_id = flds[0].split('.')[0]
                data_flds = flds[2:]

                # iterate the other elements with index
                # have the index start at 2 to match the header which also includes the first two columns
                for i, value in enumerate(data_flds, start=2):
                    tissue_detailed_name = header_fields[i]
                    self.gene_expressed_tissue.add_relationship(
                        {'sid': gene_id}, {'name': tissue_detailed_name}, {'val': value}
                    )
