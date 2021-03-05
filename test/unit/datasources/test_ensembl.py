import pytest
import pprint

from biomedgraph.datasources import Ensembl


def test_taxid_name_mapping():
    taxid_2_name, name_2_taxid = Ensembl.get_ensembl_species_names_2_taxid()

    assert taxid_2_name
    assert name_2_taxid

    assert '9606' in taxid_2_name
    assert '10090' in taxid_2_name

    assert 'homo_sapiens' in name_2_taxid
    assert 'mus_musculus' in name_2_taxid

    # the taxid_2_names dictionary is truncated and contains only
    # the taxids with one defined reference strain name
    # but all names from the taxid_2_name dictionary must be found in name_2_taxid
    assert all(x in name_2_taxid for x in taxid_2_name.values())
