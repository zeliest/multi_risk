import os
import scipy as sp
from climada.util.api_client import Client
import pandas as pd
import pycountry
import numpy as np
from climada_petals.entity.impact_funcs.river_flood import (
    ImpfRiverFlood,
    flood_imp_func_set,
    RIVER_FLOOD_REGIONS_CSV,
)
from climada.engine.impact import Impact
from climada.hazard import Hazard
from define_impf import (
    impact_fun_set_pop,
    impact_function_set_assets,
    get_impf_id,
)
from compute_tc_mit import make_tc_hazard

flood_dir = "/nfs/n2o/wcr/szelie/CLIMADA_api_data/"
OUTPUT_DIR = "/nfs/n2o/wcr/szelie/impacts_multi_risk"
path_hazard = {
    "TC": "/nfs/n2o/wcr/szelie/MIT_windfields/hazard_TC_MIT/hazard_tc_{warming}.hdf5",
    "RF": (
        '/nfs/n2o/wcr/szelie/CLIMADA_api_data/flood_warming_level/global/{warming}/river_flood_150arcsec_{warming}.hdf5'
    ),
}
file_output = (
    "/nfs/n2o/wcr/szelie/impacts_multi_risk/impacts/{country}/{ext}/"
    "{hazard}_impact_{exposure}_150arcsec_{warming}_{country}.{ext}"
)


def compute_impacts_warming_level(hazard_type, exposure_str, warming_level, country="global"):
    client = Client()
    if exposure_str == "pop":
        properties = {
            "res_arcsec": "150",
            "exponents": "(0,1)",
            "spatial_coverage": "global",
        }
        if country != "global":
            properties.update(
                {"country_iso3alpha": country, "spatial_coverage": "country"}
            )
        exposure = client.get_exposures(
            "litpop", properties=properties, version="v2"
        )
    elif exposure_str == "assets":
        properties = {
            "res_arcsec": "150",
            "exponents": "(1,1)",
            "spatial_coverage": "global",
        }
        if country != "global":
            properties.update(
                {"country_iso3alpha": country, "spatial_coverage": "country"}
            )

        exposure = client.get_exposures(
            "litpop", properties=properties, version="v2"
        )

    exposure.gdf = exposure.gdf.dropna()
    exposure.gdf = exposure.gdf.reset_index(drop=True)
    regions_df = pd.read_csv(RIVER_FLOOD_REGIONS_CSV)

    if exposure_str == "assets":
        imp_fun_set = impact_function_set_assets()
        exposure.gdf['impf_RF'] = 1
        for cnt in regions_df['ISO']:
            try:
                country_numeric = int(pycountry.countries.get(alpha_3=cnt).numeric)
                exposure.gdf.loc[exposure.gdf['region_id'] == country_numeric, 'impf_RF'] = \
                    int(regions_df[regions_df['ISO'] == cnt]['impf_RF'])
            except:
                continue

        exposure.gdf['impf_TC'] = 1
        for cnt in np.unique(exposure.gdf.region_id):
            exposure.gdf.loc[exposure.gdf['region_id']==cnt, 'impf_TC'] = get_impf_id(int(cnt))[1]
    elif exposure_str == 'pop':
        imp_fun_set = impact_fun_set_pop()
        exposure.gdf['impf_TC'] = 1
        exposure.gdf['impf_RF'] = 1

    hazard = Hazard.from_hdf5(path_hazard[hazard_type].format(warming=warming_level))
    if hazard == 'RF':
        events_rf = [event_name for event_name in hazard.event_name if 'rcp85' not in event_name]
        hazard.select(event_names=events_rf)
    impact = Impact()
    impact.calc(exposure, imp_fun_set, hazard, save_mat=True)
    output_dir_csv = os.path.join(OUTPUT_DIR, 'impacts', country, 'csv')
    output_dir_npz = os.path.join(OUTPUT_DIR, 'impacts', country, 'npz')

    isExist = os.path.exists(output_dir_csv)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(output_dir_csv)

    isExist = os.path.exists(output_dir_npz)
    if not isExist:
        os.makedirs(output_dir_npz)

    impact.write_sparse_csr(file_output.format(country=country, ext='npz', warming=warming_level,
                                                              hazard=hazard_type, exposure=exposure_str))
    impact.write_csv(file_output.format(country=country, ext='csv', warming=warming_level,
                                                              hazard=hazard_type, exposure=exposure_str))


if __name__ == "__main__":
    make_tc_hazard()
    compute_impacts_warming_level(warming_level='1', exposure_str='assets', hazard_type='TC')
    compute_impacts_warming_level(warming_level='2', exposure_str='assets', hazard_type='TC')
    compute_impacts_warming_level(warming_level='1', exposure_str='pop', hazard_type='TC')
    compute_impacts_warming_level(warming_level='2', exposure_str='pop', hazard_type='TC')
    # compute_impacts_warming_level(warming_level='1', exposure_str='assets', hazard_type='RF')
    # compute_impacts_warming_level(warming_level='2', exposure_str='assets', hazard_type='RF')
    # compute_impacts_warming_level(warming_level='1', exposure_str='pop', hazard_type='RF')
    # compute_impacts_warming_level(warming_level='2', exposure_str='pop', hazard_type='RF')




