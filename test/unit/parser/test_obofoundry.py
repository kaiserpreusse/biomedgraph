import pytest
import os

from biomedgraph.parser import OboFoundryParser


class TestObofoundryParser:
    def test_obofoundry_parser(self):
        oboparser = OboFoundryParser()
        this_path = os.path.dirname(os.path.abspath(__file__))
        oboparser.parse_obo_file(os.path.join(this_path, 'fao.obo'))

