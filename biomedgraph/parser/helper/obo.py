import os
import logging
import re

log = logging.getLogger(__name__)


def clean_obo_file(obofile):
    """
    Clean format problems.

    :param obofile: Path to OBO file.
    :return: Path to cleaned file.
    """
    log.debug(f"Clean OBI file {obofile}")
    path = os.path.dirname(obofile)
    filename = os.path.basename(obofile)

    output_filename = "temp_clean_{}".format(filename)
    output = os.path.join(path, output_filename)
    log.debug(f"Write cleaned file to {output}")

    try:
        os.remove(output)
    except Exception as e:
        log.info(e)

    # clean file, dbxref links in def line not well formatted
    first_term_found = False

    with open(output, 'wt') as out:
        with open(obofile, 'rt') as f:
            for l in f:
                # check if first term was found (= skip header)
                if not first_term_found:
                    if '[Term]' in l:
                        first_term_found = True
                if first_term_found:
                    # clean xref def
                    l = remove_space_from_xref(l)

                out.write(l)

    return output


def remove_space_from_xref(line):
    """
    Clean up xref definitions in a way that all spaces are removed or replaced by an underscore.

    - [EC: 1.2.3.4] is bad, no space after EC: [EC:1.2.3.4] is correct
    - [TAO:Arratia and Schultze_1992] is bad, replace space with underscore

    - some OBO files do not use the bracket notation

    :param line:
    :return:
    """
    # check if line contains xrefs
    line = line.strip()
    # if the line ends with ']]' there are malformatted xrefs
    # such as [ISBN 3-89937-052-X [Goujet and Young\, 2004\]]
    if line.endswith(']]'):
        if '[' in line:
            # get all occurences of '['
            all_open_brackets = [m.start() for m in re.finditer('\[', line)]
            index_second_last_open_bracket = all_open_brackets[-2]
            str_before_bracket = line[:index_second_last_open_bracket]
            str_xref_def = line[index_second_last_open_bracket:]

            line = str_before_bracket + '[' + clean_string_xref_element(str_xref_def) + ']'

    elif line.endswith(']') and line != '[Term]' and line != '[Typedef]':
        if '[' in line:
            # find last '[' bracket
            xref_open_bracket_index = line.rfind('[')

            str_before_bracket = line[:xref_open_bracket_index]
            str_xref_def = line[xref_open_bracket_index:]

            # split up the xref def
            cleaned_xref_strings = []

            # first handle lists xref defs with proper key:value
            if ':' in str_xref_def and ', ' in str_xref_def:
                for xref in str_xref_def[1:-1].split(','):
                    if ':' in xref:
                        cleaned_xref = clean_key_value_xref_element(xref)
                        # if there is wrong formatting, the cleaning function returns non
                        if cleaned_xref:
                            cleaned_xref_strings.append(cleaned_xref)
                    else:
                        cleaned_xref = clean_string_xref_element(xref)
                        # if there is wrong formatting, the cleaning function returns non
                        if cleaned_xref:
                            cleaned_xref_strings.append(cleaned_xref)

            # then handle single xref defs
            elif ':' in str_xref_def and ', ' not in str_xref_def:
                cleaned_xref = clean_key_value_xref_element(str_xref_def[1:-1])
                # if there is wrong formatting, the cleaning function returns non
                if cleaned_xref:
                    cleaned_xref_strings.append(cleaned_xref)

            else:
                cleaned_xref = clean_string_xref_element(str_xref_def[1:-1])
                if cleaned_xref:
                    cleaned_xref_strings.append(cleaned_xref)

            if cleaned_xref_strings:
                cleaned_xref_def = ''.join(['[', ', '.join(cleaned_xref_strings), ']'])
            else:
                cleaned_xref_def = '[]'

            line = str_before_bracket + cleaned_xref_def
    else:
        if 'xref: ' in line:
            xref_key, xref_string = line.split(' ', 1)
            if ':' in xref_string:
                xref_string = clean_key_value_xref_element(xref_string)
            line = f'{xref_key} {xref_string}'

    return line + '\n'


def clean_key_value_xref_element(xref):
    """
    Takes an xref element with key and value and cleans it up.

    :param xref: The xref element.
    :return: The cleaned xref element
    """
    xref = xref.strip()

    k, v = xref.split(':', 1)
    # remove ' ' in key
    k = k.replace(' ', '').replace('\\', '')

    # remove starting ' ' in value
    if v.startswith(' '):
        v = v[1:]
    # remove trailing ',' in value
    if v.endswith(','):
        v = v[:-1]
    # replace all other spaces with underscore
    v = v.replace(' ', '_').replace('\\', '')

    # some values contain subvalues (wrong formatting)
    # [GO:[GOC:mtg_sensu, ISBN:0198547684]
    # simply remove those elements
    if not v.startswith('[') and not v.endswith(']'):
        return f"{k}:{v}"


def clean_string_xref_element(xref):
    cleaned_string = xref.strip().replace(' ', '_').replace('\\', '').replace('[', '').replace(']', '')
    if "ISBN" in xref:
        print(xref)
        print(cleaned_string)

    return cleaned_string
