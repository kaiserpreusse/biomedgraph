import logging
from graphpipeline.parser import ReturnParser
from graphio import NodeSet, RelationshipSet

log = logging.getLogger(__name__)


class BigWordListParser(ReturnParser):


    def __init__(self, root_dir):
        """
        """
        super(BigWordListParser, self).__init__(root_dir)

        # NodeSets
        self.words = NodeSet(['Word'], merge_keys=['value'])

    def run_with_mounted_arguments(self):
        self.run()

    def run(self):

        log.info("Run {}".format(self.__class__.__name__))

        bigwordlist_instance = self.get_instance_by_name('BigWordList')

        # collect the words and a list of wordlists they are metioned in
        word_to_list = {}

        log.info("")
        for i in range(3,13):
            match_file = bigwordlist_instance.get_file('wlist_match{}.txt'.format(i))
            try:
                log.info("Open {}".format(match_file))
                with open(match_file, 'r') as f:
                    for l in f:
                        word = l.strip()
                        if word in word_to_list:
                            word_to_list[word].append(i)
                        else:
                            word_to_list[word] = [i]
            except TypeError:
                log.info("Cannot open file {}".format(match_file))

        for word, list_of_matches in word_to_list.items():
            node_props = {'value': word}
            for i in list_of_matches:
                node_props["match{}".format(i)] = True

            self.words.add_node(node_props)
