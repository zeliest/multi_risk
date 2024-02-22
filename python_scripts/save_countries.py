#!/usr/bin/env python3
import copy
import itertools
import os
import sys
from ast import literal_eval

hazards = ['TC', 'RF']
combinations = list(itertools.combinations(hazards, 2))

import copy
import itertools

import numpy as np
import pycountry

from climada.engine import Impact, ImpactCalc
from climada.util import coordinates
from climada.util.yearsets import set_imp_mat
from compute_compound_impacts_warming import OUTPUT_DIR

def set_imp_mat(impact, imp_mat):
    """
    Set Impact attributes from the impact matrix. Returns a copy.
    Overwrites eai_exp, at_event, aai_agg, imp_mat

    Parameters
    ----------
    impact: Impact
    imp_mat : sparse.csr_matrix
        matrix num_events x num_exp with impacts.
    Returns
    -------
    imp : Impact
        Copy of impact with eai_exp, at_event, aai_agg, imp_mat set.
    """
    imp = copy.deepcopy(impact)
    imp.at_event, imp.eai_exp, imp.aai_agg = ImpactCalc.risk_metrics(imp_mat, imp.frequency)
    imp.imp_mat = imp_mat
    return imp

def impact_select_country(impact, country):
    """Create a new impact object with only the data for the given country.

    Args:
        impact (Impact): The impact object to filter.
        country (int): The country code to filter the impact by.

    Returns:
        Impact: A new impact object containing only the data for the given country.
    """
    # Extract latitude and longitude coordinates from impact.coord_exp
    lat, lon = np.array(impact.coord_exp).T
    countries_num = coordinates.get_country_code(lat, lon)
    countries_num = np.array([format(num, '03d') for num in countries_num])
    impact_country = copy.deepcopy(impact)
    imp_mat = impact.imp_mat[:, countries_num == str(country)]
    impact_country.coord_exp = impact.coord_exp[countries_num == str(country)]
    impact_country = set_imp_mat(impact_country, imp_mat)
    return impact_country


def create_impact_files_per_country(hazards, countries, exposures, warmings):
    """Create impact files for different combinations of hazards and countries.

    Args:
        hazards (List[str]): A list of hazard types to combine.
        countries (List[int]): A list of country codes to create impact files for.
        exposures (List[str]): A list of exposure types to create impact files for.
        warmings (List[str]): A list of warming levels to create impact files for.
    """
    combinations = list(itertools.combinations(hazards, 2))

    def create_impact_file(exposure, warming, combi, impact_type, country):
        """Create an impact file for a combination of hazards and a country.

        Args:
            exposure (str): The exposure type to create the impact file for.
            warming (str): The warming level to create the impact file for.
            combi (Tuple[str, str]): The combination of hazards to create the impact file for.
            impact_type (str): The impact type to create the impact file for.
            country (int): The country code to create the impact file for.

        Returns:
            str: The path of the created impact file.
        """
        # Create combined impact for the combination of hazards
        combined_impact = Impact.from_csv(
            OUTPUT_DIR + '/combined/{}/global/csv/{}/{}_combined_impact_ordered_{}_150arcsec_{}_global.csv'.format(
                impact_type, exposure, exposure, "_".join(combi), warming
            )
        )
        combined_impact.imp_mat = Impact.read_sparse_csr(
            OUTPUT_DIR + '/combined/{}/global/npz/{}_combined_impact_ordered_{}_150arcsec_{}_global.npz'.format(
                impact_type, exposure, "_".join(combi), warming
            )
        )
        # Create impact file for the country
        country_pycountry = pycountry.countries.get(numeric=str(country))
        if country_pycountry is None:
            return
        else:
            country_alpha3 = country_pycountry.alpha_3
        impact_file = OUTPUT_DIR + '/countries/{}_{}_impact_ordered_{}_150arcsec_{}_{}.csv'.format(
            exposure, impact_type, "_".join(combi), warming, country_alpha3
        )
        if not os.path.exists(impact_file):
            # Skip calculations if the output file exists
            impact_country = impact_select_country(combined_impact, country)
            impact_country.write_csv(impact_file)

        # Create combined impact for the combination of hazards
        combined_impact = Impact.from_csv(
            OUTPUT_DIR + '/combined/{}/global/csv/{}/{}_combined_impact_{}_150arcsec_{}_global.csv'.format(
                impact_type, exposure, exposure, "_".join(combi), warming
            )
        )
        combined_impact.imp_mat = Impact.read_sparse_csr(
            OUTPUT_DIR + '/combined/{}/global/npz/{}_combined_impact_{}_150arcsec_{}_global.npz'.format(
                impact_type, exposure, "_".join(combi), warming
            )
        )        # Save combined impact to a file
        impact_file = OUTPUT_DIR + '/countries/{}_{}_impact_{}_150arcsec_{}_{}.csv'.format(
            exposure, impact_type, "_".join(combi), warming, country_alpha3
        )
        if not os.path.exists(impact_file):
            # Skip calculations if the output file exists
            impact_country = impact_select_country(combined_impact, country)
            impact_country.write_csv(impact_file)

        for hazard in combi:
            impacts_yearsets_ordered = Impact.from_csv("".join(
                [OUTPUT_DIR, '/yearsets/global/csv/', "_".join([hazard, exposure]),
                 "_impacts_yearsets_150arcsec_", warming, "_global.csv"]))
            impacts_yearsets_ordered.imp_mat = Impact.read_sparse_csr("".join(
                [OUTPUT_DIR, '/yearsets/global/npz/', "_".join([hazard, exposure]),
                 "_impacts_yearsets_150arcsec_", warming, "_global.npz"]))
            impact_file = OUTPUT_DIR + '/countries/{}_{}_impacts_yearsets_150arcsec_{}_{}.csv'.format(
                exposure, hazard, warming, country_alpha3
            )
            if not os.path.exists(impact_file):
                # Skip calculations if the output file exists
                impact_country = impact_select_country(impacts_yearsets_ordered, country)
                impact_country.write_csv(impact_file)

    for exposure in exposures:
        for warming in warmings:
            for combination_type in ['compound', 'aggr']:
                # Create impact files for the combination of hazards and countries
                [
                    create_impact_file(exposure, warming, combi, combination_type, country)
                    for combi in combinations
                    for country in countries
                ]


exposures = ['assets','pop']
# Convert the second argument to a list
warmings = ["1", "2"]

if __name__ == "__main__":
    create_impact_files_per_country(hazards=['TC', 'RF'], countries=[450], exposures=exposures, warmings=warmings)