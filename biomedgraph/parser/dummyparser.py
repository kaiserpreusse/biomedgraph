import string
from random import randint

from elderberry import ReturnParser
from graphio import NodeSet, RelationshipSet


class DummyParser(ReturnParser):


    def __init__(self, root_dir):
        super(DummyParser, self).__init__(root_dir)

        # arguments
        self.arguments = ['taxid']

        # output data
        self.dummy_nodes = NodeSet(['Dummy'], merge_keys=['sid'])
        self.fummy_nodes = NodeSet(['Fummy'], merge_keys=['sid'])

        self.dummy_knows_fummy = RelationshipSet('KNOWS', ['Dummy'], ['Fummy'], ['sid'], ['sid'])

        self.object_sets = [self.dummy_nodes, self.fummy_nodes, self.dummy_knows_fummy]
        self.container.add_all(self.object_sets)

    def run_with_mounted_arguments(self):
        self.run(self.taxid)

    def run(self, taxid):
        dummy_instance = self.get_instance_by_name('Dummy')
        dummyfile = dummy_instance.get_file('file.txt')

        target_sids = list(string.ascii_lowercase)

        # Fummy nodes
        for i in range(10):
            self.fummy_nodes.add_node({'sid': i, 'taxid': taxid})

        with open(dummyfile) as f:
            for l in f:
                letter = l.strip()
                self.dummy_nodes.add_node(
                    {'sid': letter, 'taxid': taxid}
                )
                self.dummy_knows_fummy.add_relationship(
                    {'sid': letter}, {'sid': randint(0, 9)}, {'key': 'value'}
                )
