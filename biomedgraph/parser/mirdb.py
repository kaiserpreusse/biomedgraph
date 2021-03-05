import gzip
from graphpipeline.parser import ReturnParser
from graphio import NodeSet, RelationshipSet

TAXID_2_MIRPREFIX = {'9606': 'hsa',
                     '10090': 'mmu'}


class MirdbParser(ReturnParser):

    def __init__(self):
        super(MirdbParser, self).__init__()

        # arguments
        self.arguments = ['taxid']

        # RelationshipSets
        self.mirna_targets_transcript = RelationshipSet('TARGETS', ['Mirna'], ['Transcript'], ['name'], ['sid'])

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):

        mirdb_instance = self.get_instance_by_name('Mirdb')
        mirdb_file = mirdb_instance.datasource.get_prediction_file(mirdb_instance)

        datasource_name = mirdb_instance.datasource.name
        mir_prefix = TAXID_2_MIRPREFIX[taxid]

        with gzip.open(mirdb_file, 'rt') as f:
            for l in f:
                flds = l.split()
                mir_name = flds[0]

                if mir_name.startswith(mir_prefix):
                    target = flds[1]
                    score = float(flds[2])

                    self.mirna_targets_transcript.add_relationship(
                        {'name': mir_name}, {'sid': target}, {'score': score, 'source': datasource_name}
                    )
