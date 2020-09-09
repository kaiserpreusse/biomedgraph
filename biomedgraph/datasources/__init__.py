from .ncbi_gene import NcbiGene
from .ensembl import Ensembl
from .refseq import Refseq
from .uniprot import Uniprot
from .mirbase import Mirbase
from .mirdb import Mirdb
from .mirtarbase import Mirtarbase
from .reactome import Reactome
from .ncbi_taxonomy import NcbiTaxonomy
from .mesh import Mesh
from .big_word_lists import BigWordList
from .dummy import Dummy
from .ncbi_homologene import NcbiHomoloGene
from .geneontology import GeneOntology
from .gtex import Gtex
from .hgnc import HGNC
from .obofoundry import OboFoundry
from .swisslipids import SwissLipids
from .chebi import Chebi
from .hmdb import Hmdb

import sys
import inspect
from datacroaker import BaseDataSource

current_module = sys.modules[__name__]

ALL_DATASOURCES = dict([(name, cls) for name, cls in inspect.getmembers(current_module, inspect.isclass) if
                        issubclass(cls, BaseDataSource) and cls.__name__ != BaseDataSource.__name__])
