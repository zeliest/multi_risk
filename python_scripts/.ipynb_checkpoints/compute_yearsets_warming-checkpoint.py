import logging
import random
import sys

from climada.util.multi_risk import *
from climada.util.api_client import Client
from climada.engine import Impact
from create_log_msg import log_msg
from compute_tc_mit import make_tc_hazard
from compute_impacts_warming_level import compute_impacts_warming_level

OUTPUT_DIR = "/nfs/n2o/wcr/szelie/impacts_multi_risk/yearsets/global"
INPUT_DIR = "/nfs/n2o/wcr/szelie/impacts_multi_risk/impacts/global"

correction_factor_assets = {'TC': 1, 'RF': 48.3 / 481}


def main(exposure_str='pop', warmings=None, cap_at_exp=True):

    if warmings is None:
        warmings = ['1', '2']
    # Create an empty dictionary to store the impact data
    # Create a client object
    client = Client()

    # Select the appropriate exposure data based on the exposure_str argument
    if exposure_str == 'assets':
        exposure = client.get_exposures('litpop',
                                        properties={'res_arcsec': '150', 'exponents': '(1,1)', 'fin_mode': 'pc',
                                                    'spatial_coverage': 'global'}, version='v2')
    elif exposure_str == 'pop':
        exposure = client.get_exposures('litpop',
                                        properties={'res_arcsec': '150', 'exponents': '(0,1)',
                                                    'spatial_coverage': 'global'}, version='v2')

    # Add a 'country' column to the exposure data
    # exposure.gdf['country'] = np.array([countries.get(numeric=format(int(num), '03d')).alpha_3
    #                                     for num in exposure.gdf.region_id])

    # Define a string with placeholders for the hazard type and exposure type

    # Loop over the warming levels
    exposure.gdf = exposure.gdf.dropna()
    exposure.gdf = exposure.gdf.reset_index(drop=True)
    impacts_per_year = {}
    if cap_at_exp is True:
        cap_str = "_caped"
        cap = exposure
    else:
        cap_str = ""
        cap = None
    for warming in warmings:
        # Create an empty dictionary to store the impact data for this warming level

        # Use the format() function to fill in the placeholders with the values of hazard_type and exposure_str
        # Define a string with placeholders for the file name parts
        for hazard_type in ['TC', 'RF']:
            file_name_str = INPUT_DIR + "/csv/{}_impact_{}_150arcsec_{}_global.csv"
            # Use the format() function to fill in the placeholders with the values of the file name parts
            file_name = file_name_str.format(hazard_type, exposure_str, warming)

            # Load the impact data from the file
            impacts_per_year[hazard_type] = Impact.from_csv(file_name)

            # Set the impact matrix for this impact data
            impacts_per_year[hazard_type].imp_mat = Impact.read_sparse_csr(file_name.replace('csv', 'npz'))

        #
        # set the frequency for the assets and population impact objects
        # impact[f"{hazard_type}_{exposure_str}"].frequency = np.ones(
        #     len(impact[f"{hazard_type}_{exposure_str}"].event_id)) / len(
        #     impact[f"{hazard_type}_{exposure_str}"].event_id)
        # impact[f"{hazard_type}_pop"].frequency = np.ones(len(impact[f"{hazard_type}_pop"].event_id)) / len(
        #     impact[f"{hazard_type}_pop"].event_id)

        logging.info("impact objects loaded")
        # aggregate the impact objects for the assets and population
        impacts_per_year['TC'] = aggregate_impact_from_event_name(
            impacts_per_year['TC'], exp=cap)
        LOG_FILE = "yearsets_Cc.txt"
        logging.info("events aggregated from event name")
        log_msg(f"TC even name are {impacts_per_year['TC'].event_name}\n", LOG_FILE)
        # create a list of combinations of year, GCM, and RCP
        if exposure_str == 'assets':
            for hazard_type in ['TC', 'RF']:
                impacts_per_year[hazard_type].imp_mat = impacts_per_year[hazard_type].imp_mat * \
                                                        correction_factor_assets[hazard_type]
                impacts_per_year[hazard_type] = set_imp_mat(impacts_per_year[hazard_type],
                                                            impacts_per_year[hazard_type].imp_mat)
                mask = np.abs(impacts_per_year[hazard_type].imp_mat.data) < 100
                impacts_per_year[hazard_type].imp_mat.data[mask] = 0
                impacts_per_year[hazard_type].imp_mat.eliminate_zeros()

        log_msg(f"Started computing yearsets\n", LOG_FILE)
        log_msg(f"length of rf unique event names is {len(impacts_per_year[hazard_type].event_name)} \n", LOG_FILE)

        event_names = [["_".join(event.split("_")[0:3]) for event in impacts_per_year[hazard_type].event_name] for
                       hazard_type
                       in ['TC', 'RF']]

        def common_elements(list1, list2):
            return [element for element in list1 if element in list2]

        event_names = np.unique(common_elements(event_names[0], event_names[1]))
        log_msg(f"len common event names is {len(event_names)} \n", LOG_FILE)
        log_msg(f"shape tc imp_mat {impacts_per_year['TC'].imp_mat.shape} \n", LOG_FILE)

        # modify the impact matrix for the assets

        # order the impacts by event name
        # write the impacts to CSV and NPZ files
        log_msg(f"starting combining events \n", LOG_FILE)
        impacts_yearsets = order_by_event_name(impacts_per_year, 50, list_string=event_names)
        log_msg(f"combining done \n", LOG_FILE)
        for hazard_type in ['TC', 'RF']:
            impacts_yearsets[hazard_type].write_csv(
                OUTPUT_DIR + '/csv/{}_{}_impacts_yearsets_150arcsec_{}_global{}.csv'.format(hazard_type,
                                                                                             exposure_str, warming,
                                                                                          cap_str))
            impacts_yearsets[hazard_type].write_sparse_csr(
                OUTPUT_DIR + '/npz/{}_{}_impacts_yearsets_150arcsec_{}_global{}.npz'.format(hazard_type,
                                                                                             exposure_str, warming,
                                                                                             cap_str))

if __name__ == "__main__":
    # make_tc_hazard()
    # compute_impacts_warming_level(warming_level='2', exposure_str='assets', hazard_type='RF')
    # compute_impacts_warming_level(warming_level='1', exposure_str='assets', hazard_type='RF')
    # compute_impacts_warming_level(warming_level='2', exposure_str='pop', hazard_type='RF')
    # compute_impacts_warming_level(warming_level='1', exposure_str='pop', hazard_type='RF')
    # main('assets')
    # main('pop')
    main('assets', cap_at_exp=False)
    main('pop', cap_at_exp=False)
