# BioDataGraph
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI](https://img.shields.io/pypi/v/biodatagraph)](https://pypi.org/project/biodatagraph)
[![Build Status](https://travis-ci.com/kaiserpreusse/biodatagraph.svg?branch=master)](https://travis-ci.com/kaiserpreusse/biodatagraph)


BioDataGraph is a toolset to download data from **biomedical databases** 
(such as ENSEMBL, Refseq and Uniprot) and extract data in a  format that
can be stored in the graph databases **Neo4j**. It is a framework that can be used in applications that work 
with biomedical data and Neo4j.

> :warning: **Note:** BioDataGraph is in alpha stage and was published early as part of the https://covidgraph.org project.
 
BioDataGraph works standalone now but it was carved out of a larger application. The following issues need to be addressed:

- Names of `Parser` and `DataSource` classes are not stable yet (some names only make 
sense in the context of the previous application)

## Example

```python
import py2neo
import logging

from biodatagraph.datasources import NcbiGene
from biodatagraph.parser import NcbiGeneParser

NEO4J_URL = 'bolt://localhost:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'test'
ROOT_DIR = '/path/to/dir'

if __name__ == '__main__':

    graph = py2neo.Graph(NEO4J_URL, user=NEO4J_USER, password=NEO4J_PASSWORD)

    # download datasource
    ncbigene = NcbiGene(ROOT_DIR)
    ncbigene.download()

    # run parser
    ncbigene_parser = NcbiGeneParser(ROOT_DIR)
    ncbigene_parser.run('9606')

    # load data to Neo4j
    for nodeset in ncbigene_parser.nodesets:
        nodeset.create(graph)
```

 ## Structure
 
 BioDataGraph has two main components: `DataSource` and `Parser`. 
 
 ### DataSource
 
 The `DataSource` classes take care of downloading data from public databases.
 
 ### Parser
 
 The `Parser` classes extract data from the downloaded files and create Neo4j ready data structures.
 
 ## Dependencies
 
 BioDataGraph creates Neo4j ready data in `NodeSet` and `RelationshipSet` classes from the 
 [graphio](https://github.com/kaiserpreusse/graphio) package.