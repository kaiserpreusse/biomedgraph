import logging

log = logging.getLogger(__name__)


class TaxTranslator:
    """
    Translate various different ways to represent organisms.
    """

    # manually defined translation dicts
    _ensembl_name = {
        'ailuropoda_melanoleuca': '9646',
        'anas_platyrhynchos': '8839',
        'anolis_carolinensis': '28377',
        'astyanax_mexicanus': '7994',
        'bos_taurus': '9913',
        'caenorhabditis_elegans': '6239',
        'callithrix_jacchus': '9483',
        'canis_familiaris': '9615',
        'cavia_porcellus': '10141',
        'chlorocebus_sabaeus': '60711',
        'choloepus_hoffmanni': '9358',
        'ciona_intestinalis': '7719',
        'ciona_savignyi': '51511',
        'danio_rerio': '7955',
        'dasypus_novemcinctus': '9361',
        'dipodomys_ordii': '10020',
        'drosophila_melanogaster': '7227',
        'echinops_telfairi': '9371',
        'equus_caballus': '9796',
        'erinaceus_europaeus': '9365',
        'felis_catus': '9685',
        'ficedula_albicollis': '59894',
        'gadus_morhua': '8049',
        'gallus_gallus': '9031',
        'gasterosteus_aculeatus': '69293',
        'gorilla_gorilla': '9593',
        'homo_sapiens': '9606',
        'ictidomys_tridecemlineatus': '43179',
        'latimeria_chalumnae': '7897',
        'lepisosteus_oculatus': '7918',
        'loxodonta_africana': '9785',
        'macaca_mulatta': '9544',
        'macropus_eugenii': '9315',
        'meleagris_gallopavo': '9103',
        'microcebus_murinus': '30608',
        'monodelphis_domestica': '13616',
        'mus_musculus': '10090',
        'mustela_putorius_furo': '9669',
        'myotis_lucifugus': '59463',
        'nomascus_leucogenys': '61853',
        'ochotona_princeps': '9978',
        'oreochromis_niloticus': '8128',
        'ornithorhynchus_anatinus': '9258',
        'oryctolagus_cuniculus': '9986',
        'oryzias_latipes': '8090',
        'otolemur_garnettii': '30611',
        'ovis_aries': '9940',
        'pan_troglodytes': '9598',
        'papio_anubis': '9555',
        'pelodiscus_sinensis': '13735',
        'petromyzon_marinus': '7757',
        'poecilia_formosa': '48698',
        'pongo_abelii': '9601',
        'procavia_capensis': '9813',
        'pteropus_vampyrus': '132908',
        'rattus_norvegicus': '10116',
        'saccharomyces_cerevisiae': '4932',
        'sarcophilus_harrisii': '9305',
        'sorex_araneus': '42254',
        'sus_scrofa': '9823',
        'taeniopygia_guttata': '59729',
        'takifugu_rubripes': '31033',
        'tarsius_syrichta': '9478',
        'tetraodon_nigroviridis': '99883',
        'tupaia_belangeri': '37347',
        'tursiops_truncatus': '9739',
        'vicugna_pacos': '30538',
        'xenopus_tropicalis': '8364',
        'xiphophorus_maculatus': '8083'
    }

    _short_names = {
        'hsa': '9606',
        'mmu': '10090'
    }

    def __init__(self, ncbi_taxonomy_instance):

        # prepare Taxonomy data source
        self.ncbi_taxonomy_instance = ncbi_taxonomy_instance

        self.ncbi_full_scientific_names = {}
        self.ncbi_abbr_scientific_names = {}
        self.ncbi_genus_species = {}

        # get translation data from external sources
        self._get_ncbi_tax_data()

        # collect all translation dicts
        self.translation_dicts = [self._ensembl_name, self.ncbi_full_scientific_names, self.ncbi_genus_species,
                                  self._short_names]

        # cache queries locally
        self.qcache = {}

    def _get_ncbi_tax_data(self):
        """
        Get taxonomy to scientific name from NCBI Taxonomy Data
        """

        with open(self.ncbi_taxonomy_instance.get_file('names.dmp')) as f:
            for l in f:

                flds = l.rstrip('\t|\n').split('\t|\t')

                klass = flds[3]
                if klass == 'scientific name':
                    tax_id = flds[0]
                    name = flds[1].lower()

                    # try abbreviating the name
                    try:
                        flds = name.split()
                        if len(flds) == 2:
                            initial = flds[0][0]
                            last = flds[1]
                            abbr_name = "{0} {1}".format(initial, last)
                            self.ncbi_abbr_scientific_names[abbr_name] = tax_id

                            # genus_species name
                            genus_species = "{0}_{1}".format(flds[0], flds[1])
                            self.ncbi_genus_species[genus_species] = tax_id

                    except IndexError:
                        pass

                    self.ncbi_full_scientific_names[name] = tax_id

    def translate(self, val):
        """
        Primary method to get the TaxID for a wide range of inputs.

        E.g. Homo Sapiens, H Sapiens, H. Sapiens ...


        :param val: Input string
        :return: Taxonomy ID
        """
        val = val.strip().lower()

        if val in self.qcache:
            return self.qcache[val]

        else:
            # try direct match
            for trans_dict in self.translation_dicts:
                if val in trans_dict:
                    taxid = trans_dict[val]
                    self.qcache[val] = taxid
                    return taxid

            # handle abbreviated scientific names
            flds = val.split()
            if len(flds) == 2:
                initial = flds[0].replace('.', '')
                last = flds[1]

                if len(initial) == 1:
                    abbr_name = "{0} {1}".format(initial, last)
                    if abbr_name in self.ncbi_abbr_scientific_names:
                        taxid = self.ncbi_abbr_scientific_names[abbr_name]
                        self.qcache[val] = taxid
                        return taxid

    # reverse queries: TaxID => Name
    def get_genus_species(self, taxid):
        """
        Return generic genus_species name for a taxid.

        :param taxid: The taxid.
        :return: The genus_species name.
        """
        try:
            return flip(self.ncbi_genus_species)[taxid]
        except KeyError:
            pass

    def get_ensembl_name(self, taxid):
        """
        Return the ENSEMBL tax name (homo_sapiens) for a taxid.

        :param taxid: Taxid to search for.
        :return: ENSEMBL like organism name (homo_sapiens)
        """

        ensembl_taxid_name = {v: k for k, v in self._ensembl_name.items()}

        try:
            return ensembl_taxid_name[taxid]
        except KeyError:
            pass


def flip(dictionary):
    return {v: k for k, v in dictionary.items()}
