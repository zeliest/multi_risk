import os
import pandas as pd
import pycountry
import numpy as np
import scipy as sp
from climada.util.api_client import Client
from climada_petals.entity.impact_funcs.river_flood import ImpfRiverFlood, flood_imp_func_set, RIVER_FLOOD_REGIONS_CSV
from climada.engine.impact import Impact
from climada.hazard import Hazard
from define_impf import impact_fun_set_pop, impact_function_set_assets, get_impf_id
from compute_tc_mit import make_tc_hazard

# Constants
FLOOD_DIR = "/nfs/n2o/wcr/szelie/CLIMADA_api_data/"
OUTPUT_DIR = "/nfs/n2o/wcr/szelie/impacts_multi_risk"
PATH_HAZARD = {
    "TC": "/nfs/n2o/wcr/szelie/MIT_windfields/hazard_TC_MIT/hazard_tc_{warming}.hdf5",
    "RF": '/nfs/n2o/wcr/szelie/CLIMADA_api_data/flood_warming_level/global/{warming}/river_flood_150arcsec_{warming}.hdf5'
}
FILE_OUTPUT_TEMPLATE = "/nfs/n2o/wcr/szelie/impacts_multi_risk/impacts/{country}/{ext}/{hazard}_impact_{exposure}_150arcsec_{warming}_{country}.{ext}"

def get_exposure(client, exposure_str, country="global"):
    properties = {
        "res_arcsec": "150",
        "exponents": "(0,1)" if exposure_str == "pop" else "(1,1)",
        "spatial_coverage": "global"
    }
    if country != "global":
        properties.update({
            "country_iso3alpha": country,
            "spatial_coverage": "country"
        })
    exposure = client.get_exposures("litpop", properties=properties, version="v2")
    exposure.gdf = exposure.gdf.dropna().reset_index(drop=True)
    return exposure

def prepare_exposure(exposure, exposure_str):
    regions_df = pd.read_csv(RIVER_FLOOD_REGIONS_CSV)
    imp_fun_set = impact_fun_set_pop() if exposure_str == 'pop' else impact_function_set_assets()
    exposure.gdf['impf_TC'] = 1
    exposure.gdf['impf_RF'] = 1
    if exposure_str == "assets":
        for cnt in regions_df['ISO']:
            try:
                country_numeric = int(pycountry.countries.get(alpha_3=cnt).numeric)
                exposure.gdf.loc[exposure.gdf['region_id'] == country_numeric, 'impf_RF'] = int(regions_df[regions_df['ISO'] == cnt]['impf_RF'])
            except:
                continue
        for cnt in np.unique(exposure.gdf.region_id):
            exposure.gdf.loc[exposure.gdf['region_id'] == cnt, 'impf_TC'] = get_impf_id(int(cnt))[1]
    return imp_fun_set

def compute_impacts_warming_level(hazard_type, exposure_str, warming_level, country="global"):
    client = Client()
    exposure = get_exposure(client, exposure_str, country)
    imp_fun_set = prepare_exposure(exposure, exposure_str)

    hazard = Hazard.from_hdf5(PATH_HAZARD[hazard_type].format(warming=warming_level))
    if hazard_type == 'RF':
        events_rf = [event_name for event_name in hazard.event_name if 'rcp85' not in event_name]
        hazard.select(event_names=events_rf)
    
    impact = Impact()
    impact.calc(exposure, imp_fun_set, hazard, save_mat=True)

    for ext in ['csv', 'npz']:
        output_dir = os.path.join(OUTPUT_DIR, 'impacts', country, ext)
        os.makedirs(output_dir, exist_ok=True)
        file_output = FILE_OUTPUT_TEMPLATE.format(country=country, ext=ext, warming=warming_level, hazard=hazard_type, exposure=exposure_str)
        if ext == 'csv':
            impact.write_csv(file_output)
        else:
            impact.write_sparse_csr(file_output)

if __name__ == "__main__":
    make_tc_hazard()
    for warming_level in ['1', '2']:
        for exposure_str in ['assets', 'pop']:
            for hazard_type in ['TC', 'RF']:
                compute_impacts_warming_level(hazard_type, exposure_str, warming_level)
