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
def main():
    # client = Client()
    # assets = client.get_exposures('litpop',
    #                                  properties={'res_arcsec': '150', 'exponents': '(1,1)', 'fin_mode': 'pc',
    #                                              'spatial_coverage': 'global'}, version='v2')
    # assets.gdf = assets.gdf.dropna()
    # assets.gdf = assets.gdf.reset_index(drop=True)
    #
    # client = Client()
    # population = client.get_exposures('litpop',
    #                                   properties={'res_arcsec': '150', 'exponents': '(0,1)',
    #                                               'spatial_coverage': 'global'}, version='v2')
    # exposure_dict = {'pop': population, 'assets': assets}
    occur_together_dict = {'pop': True, 'assets': True}

    for exposure in ['pop','assets']:
        for warming in ['1','2']:
            impacts_yearsets_ordered = {}

            how ='sum'
            for hazard in ['TC', 'RF']:
                impacts_yearsets_ordered[hazard] = Impact.from_csv("".join(
                    [OUTPUT_DIR, '/yearsets/global/csv/', "_".join([hazard, exposure]),
                     "_impacts_yearsets_150arcsec_",warming, "_global_caped.csv"]))
                impacts_yearsets_ordered[hazard].imp_mat = Impact.read_sparse_csr("".join(
                    [OUTPUT_DIR, '/yearsets/global/npz/', "_".join([hazard, exposure]),
                     "_impacts_yearsets_150arcsec_", warming,"_global_caped.npz"]))


            impacts_yearsets = copy.deepcopy(impacts_yearsets_ordered)
            impacts_yearsets['RF'] = order_events_by_indices(impacts_yearsets_ordered['RF'],
                                                shuffle(np.arange(len(impacts_yearsets_ordered['RF'].event_id))))
            hazards = ['TC', 'RF']
            combinations = list(itertools.combinations(hazards, 2))

            #combinations.append(list(itertools.combinations(hazards, 3)))
            for combi in combinations:
                log_msg(f"Start combining {combi} for exposure {exposure} for ordered evens.\n", LOG_FILE)
                log_msg(f"Impact matrix are of shape {impacts_yearsets_ordered[combi[0]].imp_mat.shape} and"
                        f"{impacts_yearsets_ordered[combi[1]].imp_mat.shape}.\n", LOG_FILE)

                combined_impact_ordered = combine_yearsets(
                    [impacts_yearsets_ordered[c] for c in combi], how=how, occur_together=occur_together_dict[exposure])

                combined_impact_ordered.tag = impacts_yearsets_ordered['RF'].tag
                combined_impact_ordered.unit = impacts_yearsets_ordered[hazard].unit
                combined_impact_ordered.write_csv("".join(
                    [OUTPUT_DIR, '/combined/compound/global/csv/', exposure, "_combined_impact_ordered_", "_".join(combi),
                     "_150arcsec_", warming, "_global.csv"]))

                combined_impact_ordered.write_sparse_csr("".join(
                    [OUTPUT_DIR, '/combined/compound/global/npz/', exposure,
                     "_combined_impact_ordered_", "_".join(combi),
                     "_150arcsec_", warming, "_global.npz"]))
                log_msg(f"saved combination {combi} for exposure {exposure}, "
                        f"starting combining random events.\n", LOG_FILE)
                log_msg(f"Impact matrix are of shape {impacts_yearsets[combi[0]].imp_mat.shape} and"
                        f"{impacts_yearsets[combi[1]].imp_mat.shape}.\n", LOG_FILE)
                combined_impact = combine_yearsets(
                    [impacts_yearsets[c] for c in combi], how=how,
                    occur_together=occur_together_dict[exposure])

                combined_impact.tag = impacts_yearsets_ordered['RF'].tag
                combined_impact.unit = impacts_yearsets[hazard].unit
                combined_impact.write_csv("".join(
                   [OUTPUT_DIR, '/combined/compound/global/csv/', exposure, '/', exposure, "_combined_impact_", "_".join(combi),
                     "_150arcsec_", warming,
                    "_global.csv"]))
                combined_impact.write_sparse_csr("".join(
                   [OUTPUT_DIR, '/combined/compound/global/npz/', exposure, "_combined_impact_", "_".join(combi),
                     "_150arcsec_",
                    warming, "_global.npz"]))


if __name__ == "__main__":
    main()