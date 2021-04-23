import pytest
import gzip

from biomedgraph.parser import GeneOntologyAssociationParser


@pytest.fixture(scope='session')
def gaf_file(tmpdir_factory):
    """
    Test GAF file, contains line for human and mouse (to test filtering for taxon!)

    25 lines from human
    10 lines from mouse
    """

    filename = tmpdir_factory.mktemp("parser").join("gaf_test_file.gaf.gz")

    text = """!gaf-version: 2.1
!
!The set of protein accessions included in this file is based on UniProt reference proteomes, which provide one protein per gene.
!They include the protein sequences annotated in Swiss-Prot or the longest TrEMBL transcript if there is no Swiss-Prot record.
!If a particular protein accession is not annotated with GO, then it will not appear in this file.
!
!Note that the annotation set in this file is filtered in order to reduce redundancy; the full, unfiltered set can be found in
!ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/UNIPROT/goa_uniprot_all.gz
!
!Generated: 2020-02-24 22:13
!GO-version: http://purl.obolibrary.org/obo/go/releases/2020-02-20/extensions/go-plus.owl
!
UniProtKB	A0A024R1R8	hCG_2014768		GO:0002181	PMID:21873635	IBA	PANTHER:PTN002008372|SGD:S000007246	P	HCG2014768, isoform CRA_a	hCG_2014768	protein	taxon:9606	20171102	GO_Central
UniProtKB	A0A024RBG1	NUDT4B		GO:0003723	GO_REF:0000043	IEA	UniProtKB-KW:KW-0694	F	Diphosphoinositol polyphosphate phosphohydrolase NUDT4B	NUDT4B	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A024RBG1	NUDT4B		GO:0005829	GO_REF:0000052	IDA		C	Diphosphoinositol polyphosphate phosphohydrolase NUDT4B	NUDT4B	protein	taxon:9606	20161204	HPA
UniProtKB	A0A024RBG1	NUDT4B		GO:0008486	GO_REF:0000003	IEA	EC:3.6.1.52	F	Diphosphoinositol polyphosphate phosphohydrolase NUDT4B	NUDT4B	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A024RBG1	NUDT4B		GO:0046872	GO_REF:0000043	IEA	UniProtKB-KW:KW-0479	F	Diphosphoinositol polyphosphate phosphohydrolase NUDT4B	NUDT4B	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A024RBG1	NUDT4B		GO:0052840	GO_REF:0000003	IEA	EC:3.6.1.52	F	Diphosphoinositol polyphosphate phosphohydrolase NUDT4B	NUDT4B	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A024RBG1	NUDT4B		GO:0052842	GO_REF:0000003	IEA	EC:3.6.1.52	F	Diphosphoinositol polyphosphate phosphohydrolase NUDT4B	NUDT4B	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A075B6H5	TRBV20OR9-2		GO:0005886	PMID:21873635	IBA	MGI:MGI:98596|PANTHER:PTN000588091	C	Ig-like domain-containing protein	TRBV20OR9-2	protein	taxon:9606	20171207	GO_Central
UniProtKB	A0A075B6H5	TRBV20OR9-2		GO:0007166	PMID:21873635	IBA	MGI:MGI:98608|PANTHER:PTN000588091	P	Ig-like domain-containing protein	TRBV20OR9-2	protein	taxon:9606	20171207	GO_Central
UniProtKB	A0A075B6H7	IGKV3-7		GO:0002250	GO_REF:0000043	IEA	UniProtKB-KW:KW-1064	P	Probable non-functional immunoglobulin kappa variable 3-7	IGKV3-7	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A075B6H7	IGKV3-7		GO:0002377	PMID:21873635	IBA	MGI:MGI:98936|PANTHER:PTN000587099	P	Probable non-functional immunoglobulin kappa variable 3-7	IGKV3-7	protein	taxon:9606	20170228	GO_Central
UniProtKB	A0A075B6H7	IGKV3-7		GO:0005615	PMID:21873635	IBA	PANTHER:PTN000587099|UniProtKB:P01619	C	Probable non-functional immunoglobulin kappa variable 3-7	IGKV3-7	protein	taxon:9606	20170228	GO_Central
UniProtKB	A0A075B6H7	IGKV3-7		GO:0005886	GO_REF:0000044	IEA	UniProtKB-SubCell:SL-0039	C	Probable non-functional immunoglobulin kappa variable 3-7	IGKV3-7	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A075B6H7	IGKV3-7		GO:0006955	PMID:21873635	IBA	MGI:MGI:98936|PANTHER:PTN000587099	P	Probable non-functional immunoglobulin kappa variable 3-7	IGKV3-7	protein	taxon:9606	20170228	GO_Central
UniProtKB	A0A075B6H7	IGKV3-7		GO:0019814	GO_REF:0000043	IEA	UniProtKB-KW:KW-1280	C	Probable non-functional immunoglobulin kappa variable 3-7	IGKV3-7	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A075B6H8	IGKV1D-42		GO:0002250	GO_REF:0000043	IEA	UniProtKB-KW:KW-1064	P	Probable non-functional immunoglobulin kappa variable 1D-42	IGKV1D-42	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A075B6H8	IGKV1D-42		GO:0002377	PMID:21873635	IBA	MGI:MGI:98936|PANTHER:PTN000587099	P	Probable non-functional immunoglobulin kappa variable 1D-42	IGKV1D-42	protein	taxon:9606	20170228	GO_Central
UniProtKB	A0A075B6H8	IGKV1D-42		GO:0005615	PMID:21873635	IBA	PANTHER:PTN000587099|UniProtKB:P01619	C	Probable non-functional immunoglobulin kappa variable 1D-42	IGKV1D-42	protein	taxon:9606	20170228	GO_Central
UniProtKB	A0A075B6H8	IGKV1D-42		GO:0005886	GO_REF:0000044	IEA	UniProtKB-SubCell:SL-0039	C	Probable non-functional immunoglobulin kappa variable 1D-42	IGKV1D-42	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A075B6H8	IGKV1D-42		GO:0006955	PMID:21873635	IBA	MGI:MGI:98936|PANTHER:PTN000587099	P	Probable non-functional immunoglobulin kappa variable 1D-42	IGKV1D-42	protein	taxon:9606	20170228	GO_Central
UniProtKB	A0A075B6H8	IGKV1D-42		GO:0019814	GO_REF:0000043	IEA	UniProtKB-KW:KW-1280	C	Probable non-functional immunoglobulin kappa variable 1D-42	IGKV1D-42	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A075B6H9	IGLV4-69		GO:0002250	GO_REF:0000043	IEA	UniProtKB-KW:KW-1064	P	Immunoglobulin lambda variable 4-69	IGLV4-69	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A075B6H9	IGLV4-69		GO:0002377	PMID:21873635	IBA	MGI:MGI:98936|PANTHER:PTN000587099	P	Immunoglobulin lambda variable 4-69	IGLV4-69	protein	taxon:9606	20170228	GO_Central
UniProtKB	A0A075B6H9	IGLV4-69		GO:0005615	PMID:21873635	IBA	PANTHER:PTN000587099|UniProtKB:P01619	C	Immunoglobulin lambda variable 4-69	IGLV4-69	protein	taxon:9606	20170228	GO_Central
UniProtKB	A0A075B6H9	IGLV4-69		GO:0005886	GO_REF:0000044	IEA	UniProtKB-SubCell:SL-0039	C	Immunoglobulin lambda variable 4-69	IGLV4-69	protein	taxon:9606	20200222	UniProt
UniProtKB	A0A075B5I2	Trbv4		GO:0005886	PMID:21873635	IBA	MGI:MGI:98596|PANTHER:PTN000588091	C	Ig-like domain-containing protein	Trbv4	protein	taxon:10090	20171207	GO_Central
UniProtKB	A0A075B5I2	Trbv4		GO:0007166	PMID:21873635	IBA	MGI:MGI:98608|PANTHER:PTN000588091	P	Ig-like domain-containing protein	Trbv4	protein	taxon:10090	20171207	GO_Central
UniProtKB	A0A075B5I3	Trbv13-1		GO:0005886	PMID:21873635	IBA	MGI:MGI:98596|PANTHER:PTN000588091	C	Ig-like domain-containing protein	Trbv13-1	protein	taxon:10090	20171207	GO_Central
UniProtKB	A0A075B5I3	Trbv13-1		GO:0007166	PMID:21873635	IBA	MGI:MGI:98608|PANTHER:PTN000588091	P	Ig-like domain-containing protein	Trbv13-1	protein	taxon:10090	20171207	GO_Central
UniProtKB	A0A075B5I8	Trbv21		GO:0005886	PMID:21873635	IBA	MGI:MGI:98596|PANTHER:PTN000588091	C	IGv domain-containing protein	Trbv21	protein	taxon:10090	20171207	GO_Central
UniProtKB	A0A075B5I8	Trbv21		GO:0007166	PMID:21873635	IBA	MGI:MGI:98608|PANTHER:PTN000588091	P	IGv domain-containing protein	Trbv21	protein	taxon:10090	20171207	GO_Central
UniProtKB	A0A075B5I9	Trbv23		GO:0005886	PMID:21873635	IBA	MGI:MGI:98596|PANTHER:PTN000588091	C	Ig-like domain-containing protein	Trbv23	protein	taxon:10090	20171207	GO_Central
UniProtKB	A0A075B5I9	Trbv23		GO:0007166	PMID:21873635	IBA	MGI:MGI:98608|PANTHER:PTN000588091	P	Ig-like domain-containing protein	Trbv23	protein	taxon:10090	20171207	GO_Central
UniProtKB	A0A075B5J0	Trbv26		GO:0001772	PMID:15128768	IDA		C	Ig-like domain-containing protein	Trbv26	protein	taxon:10090	20090113	MGI	part_of(CL:0000084)
UniProtKB	A0A075B5J0	Trbv26		GO:0005886	PMID:21873635	IBA	MGI:MGI:98596|PANTHER:PTN000588091	C	Ig-like domain-containing protein	Trbv26	protein	taxon:10090	20171207	GO_Central		"""

    with gzip.open(filename, 'wt') as f:
        f.write(text)

    return filename


# class TestGeneOntologyAssociationParser:
#
#     def test_gaf_parser_human(self, gaf_file):
#
#         parser = GeneOntologyAssociationParser()
#
#         parser.parse_goa_uniprot_gaf_file(gaf_file, '9606')
#
#         assert len(parser.protein_associates_goterm.relationships) == 25
#
#         for rel in parser.protein_associates_goterm.relationships:
#             assert type(rel.start_node_properties['sid']) is str
#             assert 'GO' in rel.end_node_properties['sid']
#
#     def test_gaf_parser_mouse(self, gaf_file):
#         parser = GeneOntologyAssociationParser()
#
#         parser.parse_goa_uniprot_gaf_file(gaf_file, '10090')
#
#         assert len(parser.protein_associates_goterm.relationships) == 10
#
#         for rel in parser.protein_associates_goterm.relationships:
#             assert type(rel.start_node_properties['sid']) is str
#             assert 'GO' in rel.end_node_properties['sid']
