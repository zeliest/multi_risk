import itertools
import logging
from sklearn.utils import shuffle
from climada.util.multi_risk import *
from climada.util.api_client import Client
from climada.engine import Impact
from create_log_msg import log_msg

OUTPUT_DIR = "/nfs/n2o/wcr/szelie/impacts_multi_risk"

# to do
# eq/tc tc/rf rf/eq
# all

LOG_FILE = 'compute_combined_impact_logs.txt'

def main(cap_at_exp=True):
    client = Client()
    occur_together_dict = {'pop': False, 'assets': False}

    for exposure in ['assets']:
        for warming in ['1', '2']:
            for cap_at_exp in [True, False]:
                if cap_at_exp:
                    assets = client.get_exposures('litpop',
                                                  properties={'res_arcsec': '150', 'exponents': '(1,1)',
                                                              'fin_mode': 'pc',
                                                              'spatial_coverage': 'global'}, version='v2')
                    assets.gdf = assets.gdf.dropna()
                    assets.gdf = assets.gdf.reset_index(drop=True)

                    # client = Client()
                    pop = client.get_exposures('litpop',
                                               properties={'res_arcsec': '150', 'exponents': '(0,1)',
                                                           'spatial_coverage': 'global'}, version='v2')
                    exposure_dict = {'pop': pop, 'assets': assets}
                    cap_str = "_caped"
                else:
                    exposure_dict = {'pop': None, 'assets': None}

                    cap_str = ""

                csv_path = {}
                npz_path = {}
                csv_path[
                    'TC'] = f"{OUTPUT_DIR}/yearsets/global/csv/TC_{exposure}_impacts_yearsets_150arcsec_{warming}_global{cap_str}.csv"
                npz_path[
                    'TC'] = f"{OUTPUT_DIR}/yearsets/global/npz/TC_{exposure}_impacts_yearsets_150arcsec_{warming}_global{cap_str}.npz"
                csv_path[
                    'RF'] = f"{OUTPUT_DIR}/yearsets/global/csv/RF_{exposure}_impacts_yearsets_150arcsec_{warming}_global.csv"
                npz_path[
                    'RF'] = f"{OUTPUT_DIR}/yearsets/global/npz/RF_{exposure}_impacts_yearsets_150arcsec_{warming}_global.npz"

                impacts_yearsets_ordered = {}

                how ='sum'

                for hazard in ['TC', 'RF']:

                    # Construct file paths using the hazard, exposure, warming, and output directory variables

                    # Load impact data from the CSV and NPZ files into an Impact object
                    impact = Impact.from_csv(csv_path[hazard])
                    impact.imp_mat = Impact.read_sparse_csr(npz_path[hazard])

                    # Add the Impact object to a dictionary, using the hazard as the key
                    impacts_yearsets_ordered[hazard] = impact
                impacts_yearsets = copy.deepcopy(impacts_yearsets_ordered)
                if cap_at_exp is True:
                    rf_not_ordered = order_events_by_indices(impacts_yearsets_ordered['RF'],
                                                        shuffle(np.arange(len(impacts_yearsets_ordered['RF'].event_id))))
                impacts_yearsets['RF'] = rf_not_ordered
                hazards = ['TC', 'RF']
                combinations = list(itertools.combinations(hazards, 2))

                #combinations.append(list(itertools.combinations(hazards, 3)))
                for combi in combinations:
                    log_msg(f"Start combining {combi} for exposure {exposure} for ordered evens.\n", LOG_FILE)
                    log_msg(f"Impact matrix are of shape {impacts_yearsets_ordered[combi[0]].imp_mat.shape} and"
                            f"{impacts_yearsets_ordered[combi[1]].imp_mat.shape}.\n", LOG_FILE)

                    combined_impact_ordered = combine_yearsets(
                        [impacts_yearsets_ordered[c] for c in combi], how=how, occur_together=occur_together_dict[exposure],
                    exposures=exposure_dict[exposure])

                    combined_impact_ordered.tag = impacts_yearsets_ordered['RF'].tag
                    combined_impact_ordered.unit = impacts_yearsets_ordered[hazard].unit
                    combined_csv_filename = "{}/combined/aggr/global/csv/{}/{}_combined_impact_ordered_{}_150arcsec_{}_global{}.csv".format(
                        OUTPUT_DIR,exposure, exposure, "_".join(combi), warming, cap_str
                    )
                    combined_impact_ordered.write_csv(combined_csv_filename)

                    combined_npz_filename = "{}/combined/aggr/global/npz/{}/{}_combined_impact_ordered_{}_150arcsec_{}_global{}.npz".format(
                        OUTPUT_DIR, exposure, exposure, "_".join(combi), warming, cap_str
                    )
                    combined_impact_ordered.write_sparse_csr(combined_npz_filename)
                    log_msg(f"saved combination {combi} for exposure {exposure}, "
                            f"starting combining random events.\n", LOG_FILE)
                    log_msg(f"Impact matrix are of shape {impacts_yearsets[combi[0]].imp_mat.shape} and"
                            f"{impacts_yearsets[combi[1]].imp_mat.shape}.\n", LOG_FILE)
                    combined_impact = combine_yearsets(
                        [impacts_yearsets[c] for c in combi], how=how,
                        occur_together=occur_together_dict[exposure], exposures=exposure_dict[exposure])

                    combined_impact.tag = impacts_yearsets_ordered['RF'].tag
                    combined_impact.unit = impacts_yearsets[hazard].unit
                    combined_csv_filename = "{}/combined/aggr/global/csv/{}/{}_combined_impact_{}_150arcsec_{}_global{}.csv".format(
                        OUTPUT_DIR, exposure, exposure, "_".join(combi), warming, cap_str
                    )
                    combined_impact.write_csv(combined_csv_filename)

                    combined_npz_filename = "{}/combined/aggr/global/npz/{}/{}_combined_impact_{}_150arcsec_{}_global{}.npz".format(
                        OUTPUT_DIR, exposure, exposure, "_".join(combi), warming, cap_str
                    )
                    combined_impact.write_sparse_csr(combined_npz_filename)


if __name__ == "__main__":
    main(cap_at_exp=True)
   # main(cap_at_exp=False)