from .ncbigene import NcbiGeneParser
from .ensembl import EnsemblEntityParser, EnsemblMappingParser, EnsemblLocusParser
from .refseq import RefseqEntityParser, RefseqCodesParser
from .uniprot import UniprotKnowledgebaseParser
from .mirbase import MirbaseParser
from .mirdb import MirdbParser
from .mirtarbase import MirtarbaseParser
from .reactome import ReactomePathwayParser, ReactomeMappingParser
from .mesh import MeshParser
from .big_word_list import BigWordListParser
from .dummyparser import DummyParser
from .ncbi_homologene import NcbiHomoloGeneParser
from .ncbigene import NcbiGeneOrthologParser
from .geneontology import GeneOntologyAssociationParser
from .gtex import GtexMetadataParser, GtexDataParser
from .hgnc import HGNCParser
from .obofoundry import OboFoundryParser
from .swisslipids import SwissLipidsParser
from .chebi import ChebiParser
from .hmdb import HmdbParser

from graphpipeline.parser import Parser
import sys
import inspect

current_module = sys.modules[__name__]

ALL_PARSER = dict([(name, cls) for name, cls in inspect.getmembers(current_module, inspect.isclass) if
                   issubclass(cls, Parser) and cls.__name__ != Parser.__name__])
