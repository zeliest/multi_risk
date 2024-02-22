import itertools
import logging
from sklearn.utils import shuffle
from climada.util.multi_risk import *
from climada.util.api_client import Client
from climada.engine import Impact
from create_log_msg import log_msg
import copy

OUTPUT_DIR = "/nfs/n2o/wcr/szelie/impacts_multi_risk"

# to do
# eq/tc tc/rf rf/eq
# all

def main():
    client = Client()
    assets = client.get_exposures('litpop',
                                     properties={'res_arcsec': '150', 'exponents': '(1,1)', 'fin_mode': 'pc',
                                                 'spatial_coverage': 'global'}, version='v2')
    pop = client.get_exposures('litpop',
                                    properties={'res_arcsec': '150', 'exponents': '(0,1)',
                                                'spatial_coverage': 'global'}, version='v2')

    exposure_dict = {'pop': pop, 'assets': assets}

    for exposure in ['pop']:
        exposure_dict[exposure].gdf = exposure_dict[exposure].gdf.dropna()
        exposure_dict[exposure].gdf = exposure_dict[exposure].gdf.reset_index(drop=True)
        for warming in ['1','2']:
            impacts_yearsets_ordered = {}

            for hazard in ['TC']:
                impacts_yearsets_ordered[hazard] = Impact.from_csv("".join(
                    [OUTPUT_DIR, '/yearsets/global/csv/', "_".join([hazard, exposure]),
                     "_impacts_yearsets_150arcsec_",warming, "_global.csv"]))
                impacts_yearsets_ordered[hazard].imp_mat = Impact.read_sparse_csr("".join(
                    [OUTPUT_DIR, '/yearsets/global/npz/', "_".join([hazard, exposure]),
                     "_impacts_yearsets_150arcsec_", warming,"_global.npz"]))
                imp_mat = impacts_yearsets_ordered[hazard].imp_mat
                m1 = imp_mat.data
                m2 = exposure_dict[exposure].gdf.value[imp_mat.nonzero()[1]]
                imp_mat = sp.sparse.csr_matrix((np.minimum(m1, m2), imp_mat.indices, imp_mat.indptr), shape=imp_mat.shape)
                #log_msg(f"impact matrix is of shape {m2.shape}\n", LOG_FILE)
                imp_mat.eliminate_zeros()
                imp = copy.deepcopy(impacts_yearsets_ordered[hazard])
                imp = set_imp_mat(imp, imp_mat)
                imp.frequency = np.ones(imp_mat.shape[0]) / imp_mat.shape[0]
                imp.date = np.arange(1, len(imp.at_event) + 1)
                imp.event_id = np.arange(1, len(imp.at_event) + 1)
                imp.tag['yimp object'] = True

                imp.write_csv(
                    OUTPUT_DIR + '/yearsets/global/csv/{}_{}_impacts_yearsets_150arcsec_{}_global{}.csv'.format(hazard,
                                                                                                exposure, warming,
                                                                                                "_caped"))
                imp.write_sparse_csr(
                    OUTPUT_DIR + '/yearsets/global/npz/{}_{}_impacts_yearsets_150arcsec_{}_global{}.npz'.format(hazard,
                                                                                                exposure, warming,
                                                                                                "_caped"))

if __name__ == "__main__":
    main()