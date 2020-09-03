import pandas
import logging

from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)

FILE_NAME = 'miRTarBase_MTI.xlsx'


# strip() fields in df!!

class MirtarbaseParser(ReturnParser):

    def __init__(self, root_dir):
        """
        """
        super(MirtarbaseParser, self).__init__(root_dir)

        # RelationshipSets
        self.mirna_targets_gene = RelationshipSet('TARGETS', ['Mirna'], ['Gene'], ['name'], ['sid'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):
        log.debug("Run {}".format(self.__class__.__name__))

        mirtarbase_instance = self.get_instance_by_name('Mirtarbase')

        mirtarbase_file = mirtarbase_instance.get_file(FILE_NAME)

        df = pandas.read_excel(mirtarbase_file, index_col=None, header=0)
        # rename columns for easier access
        df.columns = ['mirtarbase_id', 'mirna', 'species_mirna', 'target_genesymbol', 'target_entrez', 'species_target',
                      'experiments', 'support_type', 'references']

        for row in df.itertuples():
            self.mirna_targets_gene.add_relationship(
                {'name': row.mirna.strip()}, {'sid': str(row.target_entrez).strip()},
                {'experiments': row.experiments, 'support_type': row.support_type, 'references': row.references,
                 'source': mirtarbase_instance.datasource.name}
            )
