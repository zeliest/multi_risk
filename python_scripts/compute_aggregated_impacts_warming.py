import itertools
import logging
import numpy as np
import copy
from climada.engine import Impact
from create_log_msg import log_msg
from sklearn.utils import shuffle
from climada.util.api_client import Client
from multi_risk import *

OUTPUT_DIR = "/nfs/n2o/wcr/szelie/impacts_multi_risk"
LOG_FILE = 'compute_combined_impact_logs.txt'

def get_exposure_data(client, exposure_type, cap_at_exp):
    """
    Fetch and prepare exposure data based on the specified type and capping option.
    
    Parameters:
    - client (Client): An instance of the API client to fetch exposure data.
    - exposure_type (str): Type of exposure ('pop' or 'assets').
    - cap_at_exp (bool): Indicates whether the impact data should be capped at exposure levels.
    
    Returns:
    - dict: A dictionary containing the exposure data.
    """
    properties = {'res_arcsec': '150', 'spatial_coverage': 'global'}
    if exposure_type == 'assets':
        properties.update({'exponents': '(1,1)', 'fin_mode': 'pc'})
    else:  # 'pop'
        properties.update({'exponents': '(0,1)'})
    
    exposure_data = client.get_exposures('litpop', properties=properties, version='v2')
    exposure_data.gdf = exposure_data.gdf.dropna().reset_index(drop=True)
    return {'pop': exposure_data if exposure_type == 'pop' else None,
            'assets': exposure_data if exposure_type == 'assets' else None}

def load_impact_data(hazard, exposure, warming, cap_str):
    """
    Load impact data for a specific hazard, exposure, and warming scenario.
    
    Parameters:
    - hazard (str): Type of hazard ('TC' or 'RF').
    - exposure (str): Type of exposure ('pop' or 'assets').
    - warming (str): Warming scenario ('1' or '2').
    - cap_str (str): String indicating whether impacts are capped ('_caped' or '').
    
    Returns:
    - Impact: An instance of the Impact class loaded with data.
    """
    csv_path = f"{OUTPUT_DIR}/yearsets/global/csv/{hazard}_{exposure}_impacts_yearsets_150arcsec_{warming}_global{cap_str}.csv"
    npz_path = f"{OUTPUT_DIR}/yearsets/global/npz/{hazard}_{exposure}_impacts_yearsets_150arcsec_{warming}_global{cap_str}.npz"
    
    impact = Impact.from_csv(csv_path)
    impact.imp_mat = Impact.read_sparse_csr(npz_path)
    return impact

def save_combined_impact(combined_impact, exposure, combi, warming, cap_str):
    """
    Save the combined impact data to CSV and NPZ files.
    
    Parameters:
    - combined_impact (Impact): The combined impact object to be saved.
    - exposure (str): Type of exposure.
    - combi (tuple): Tuple of combined hazards.
    - warming (str): Warming scenario.
    - cap_str (str): String indicating whether impacts are capped.
    """
    base_path = f"{OUTPUT_DIR}/combined/aggr/global"
    combi_str = "_".join(combi)
    csv_filename = f"{base_path}/csv/{exposure}/{exposure}_combined_impact_{combi_str}_150arcsec_{warming}_global{cap_str}.csv"
    npz_filename = f"{base_path}/npz/{exposure}/{exposure}_combined_impact_{combi_str}_150arcsec_{warming}_global{cap_str}.npz"
    
    combined_impact.write_csv(csv_filename)
    combined_impact.write_sparse_csr(npz_filename)
    log_msg(f"Saved combined impact for {combi_str} under exposure {exposure}, warming {warming}.\n", LOG_FILE)

def combine_impacts_and_log(impacts_yearsets_ordered, occur_together_dict, exposure_dict, exposure, warming, cap_str):
    """
    Combine impacts for different hazards, log messages, and save the results.
    
    Parameters:
    - impacts_yearsets_ordered (dict): Dictionary of ordered Impact objects.
    - occur_together_dict (dict): Dictionary indicating if events occur together based on exposure.
    - exposure_dict (dict): Dictionary containing exposure data.
    - exposure (str): Type of exposure.
    - warming (str): Warming scenario.
    - cap_str (str): String indicating whether impacts are capped.
    """
    hazards = ['TC', 'RF']
    combinations = list(itertools.combinations(hazards, 2))
    
    for combi in combinations:
        log_msg(f"Start combining {combi} for exposure {exposure}.\n", LOG_FILE)
        
        combined_impact_ordered = combine_yearsets(
            [impacts_yearsets_ordered[c] for c in combi], how='sum',
            occur_together=occur_together_dict[exposure], exposures=exposure_dict[exposure]
        )
        
        combined_impact_ordered.tag = impacts_yearsets_ordered['RF'].tag
        combined_impact_ordered.unit = impacts_yearsets_ordered[combi[0]].unit
        save_combined_impact(combined_impact_ordered, exposure, combi, warming, cap_str)

def main(cap_at_exp=True):
    """
    Main function to compute combined impacts for given exposure settings.
    
    Parameters:
    - cap_at_exp (bool): Flag to indicate whether to cap impacts at exposure levels each year. Defaults to True.
    """
    client = Client()
    occur_together_dict = {'pop': False, 'assets': False}
    
    for exposure in ['assets']:
        for warming in ['1', '2']:
            cap_str = "_caped" if cap_at_exp else ""
            exposure_dict = get_exposure_data(client, exposure, cap_at_exp)
            
            impacts_yearsets_ordered = {
                hazard: load_impact_data(hazard, exposure, warming, cap_str) for hazard in ['TC', 'RF']
            }
            
            # Example function to shuffle and reorder events, similar to the original code structure
            # impacts_yearsets_ordered['RF'] = reorder_events(impacts_yearsets_ordered['RF'])
            
            combine_impacts_and_log(impacts_yearsets_ordered, occur_together_dict, exposure_dict, exposure, warming, cap_str)

if __name__ == "__main__":
    main(cap_at_exp=True)
