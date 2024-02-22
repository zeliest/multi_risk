import itertools

import numpy as np

from climada.engine import Impact
from compute_compound_impacts_warming import OUTPUT_DIR

hazards = ['TC', 'RF']
combinations = list(itertools.combinations(hazards, 2))
for exposure in ['pop', 'assets']:
    for warming in ['1']:
        for combi in combinations:

            combined_impact = Impact.from_csv("".join(
                       [OUTPUT_DIR, '/combined/compound/global/csv/', exposure, '/', exposure, "_combined_impact_ordered_", "_".join(combi),
                         "_150arcsec_", warming,
                        "_global.csv"]))
            combined_impact.imp_mat = Impact.read_sparse_csr("".join(
                       [OUTPUT_DIR, '/combined/compound/global/npz/', exposure, "_combined_impact_ordered_", "_".join(combi),
                         "_150arcsec_",
                        warming, "_global.npz"]))
            impacts_yearsets_ordered = {}
            fq = combined_impact.calc_freq_curve()
            condition = combined_impact.at_event > fq.impact[fq.return_per > 100][1]
            event = np.where(condition)[0][0]
            eai_exp = np.squeeze(np.asarray(combined_impact.imp_mat[event,:].todense()))
            combined_impact = combined_impact.select(event_ids=combined_impact.event_id[event])
            combined_impact.eai_exp = eai_exp
            combined_impact.write_csv("".join([OUTPUT_DIR, '/100y_event/', exposure, "_100y_event_compound_ordered_","_".join(combi),
                         "_150arcsec_", warming,
                        "_global.csv"]))

            for hazard in ['TC', 'RF']:
                impacts_yearsets_ordered[hazard] = Impact.from_csv("".join(
                    [OUTPUT_DIR, '/yearsets/global/csv/', "_".join([hazard, exposure]),
                     "_impacts_yearsets_150arcsec_", warming, "_global_caped.csv"]))
                impacts_yearsets_ordered[hazard].imp_mat = Impact.read_sparse_csr("".join(
                    [OUTPUT_DIR, '/yearsets/global/npz/', "_".join([hazard, exposure]),
                     "_impacts_yearsets_150arcsec_", warming, "_global_caped.npz"]))
                eai_exp = np.squeeze(np.asarray(impacts_yearsets_ordered[hazard].imp_mat[event,:].todense()))
                impacts_yearsets_ordered[hazard] = impacts_yearsets_ordered[hazard].select(event_ids=impacts_yearsets_ordered[hazard].event_id[event])
                impacts_yearsets_ordered[hazard].eai_exp = eai_exp

                impacts_yearsets_ordered[hazard].write_csv("".join(
                [OUTPUT_DIR, '/100y_event/',hazard,'_',exposure, "_100y_event_yearsets_",
                 "_150arcsec_", warming,
                 "_global.csv"]))

            # combined_impact = Impact.from_csv("".join(
            #     [OUTPUT_DIR, '/combined/aggr/global/csv/', exposure, '/', exposure, "_combined_impact_ordered_",
            #      "_".join(combi),
            #      "_150arcsec_", warming,
            #      "_global.csv"]))
            # combined_impact.imp_mat = Impact.read_sparse_csr("".join(
            #     [OUTPUT_DIR, '/combined/aggr/global/npz/', exposure, "_combined_impact_ordered_",
            #      "_".join(combi),
            #      "_150arcsec_",
            #      warming, "_global.npz"]))
            #
            # combined_impact.eai_exp = np.squeeze(np.asarray(combined_impact.imp_mat[event, :].todense()))
            # combined_impact.write_csv(
            #     "".join([OUTPUT_DIR, '/100y_event/', exposure, "_100y_event_aggr_ordered_", "_".join(combi),
            #              "_150arcsec_", warming,
            #              "_global.csv"]))
