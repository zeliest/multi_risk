import logging
import numpy as np
import sys

from climada.util.api_client import Client
from climada.engine import Impact
from create_log_msg import log_msg
from compute_tc_mit import make_tc_hazard

OUTPUT_DIR = "/nfs/n2o/wcr/szelie/impacts_multi_risk/yearsets/global"
INPUT_DIR = "/nfs/n2o/wcr/szelie/impacts_multi_risk/impacts/global"
LOG_FILE = "yearsets_Cc.txt"

correction_factor_assets = {'TC': 1, 'RF': 48.3 / 481}

def load_exposure_data(client, exposure_type):
    properties = {'res_arcsec': '150', 'spatial_coverage': 'global', 
                  'exponents': '(1,1)' if exposure_type == 'assets' else '(0,1)',
                  'fin_mode': 'pc' if exposure_type == 'assets' else None}
    exposure = client.get_exposures('litpop', properties=properties, version='v2')
    exposure.gdf = exposure.gdf.dropna().reset_index(drop=True)
    return exposure

def load_and_prepare_impact(hazard_type, exposure_str, warming, cap_at_exp):
    cap_str = "_caped" if cap_at_exp else ""
    file_path = f"{INPUT_DIR}/csv/{hazard_type}_impact_{exposure_str}_150arcsec_{warming}_global.csv"
    impact = Impact.from_csv(file_path)
    impact.imp_mat = Impact.read_sparse_csr(file_path.replace('csv', 'npz'))
    return impact, cap_str

def apply_corrections_and_mask(impacts_per_year):
    for hazard_type, correction_factor in correction_factor_assets.items():
        impacts_per_year[hazard_type].imp_mat *= correction_factor
        mask = np.abs(impacts_per_year[hazard_type].imp_mat.data) < 100
        impacts_per_year[hazard_type].imp_mat.data[mask] = 0
        impacts_per_year[hazard_type].imp_mat.eliminate_zeros()

def common_elements(list1, list2):
    return [element for element in list1 if element in list2]

def main(exposure_str='pop', warmings=None, cap_at_exp=True):
    if warmings is None:
        warmings = ['1', '2']
    client = Client()
    exposure = load_exposure_data(client, exposure_str)
    cap = exposure if cap_at_exp else None

    for warming in warmings:
        impacts_per_year = {}
        for hazard_type in ['TC', 'RF']:
            impacts_per_year[hazard_type], cap_str = load_and_prepare_impact(hazard_type, exposure_str, warming, cap_at_exp)

        logging.info("Impact objects loaded")

        if exposure_str == 'assets':
            apply_corrections_and_mask(impacts_per_year)

        # Additional processing such as event name manipulation and impact ordering can go here

        for hazard_type in ['TC', 'RF']:
            csv_path = f"{OUTPUT_DIR}/csv/{hazard_type}_{exposure_str}_impacts_yearsets_150arcsec_{warming}_global{cap_str}.csv"
            npz_path = f"{OUTPUT_DIR}/npz/{hazard_type}_{exposure_str}_impacts_yearsets_150arcsec_{warming}_global{cap_str}.npz"
            impacts_per_year[hazard_type].write_csv(csv_path)
            impacts_per_year[hazard_type].write_sparse_csr(npz_path)
            log_msg(f"Processed and saved data for {hazard_type}, warming level {warming}, exposure {exposure_str}.\n", LOG_FILE)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main('assets', cap_at_exp=False)
    main('pop', cap_at_exp=False)
