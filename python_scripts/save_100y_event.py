import itertools
import numpy as np
from climada.engine import Impact
from compute_compound_impacts_warming import OUTPUT_DIR

def load_and_process_combined_impact(exposure, combi, warming):
    """Load and process combined impact data for given exposure, hazard combination, and warming scenario."""
    combined_impact = Impact.from_csv(f"{OUTPUT_DIR}/combined/compound/global/csv/{exposure}/{exposure}_combined_impact_ordered_{'_'.join(combi)}_150arcsec_{warming}_global.csv")
    combined_impact.imp_mat = Impact.read_sparse_csr(f"{OUTPUT_DIR}/combined/compound/global/npz/{exposure}_combined_impact_ordered_{'_'.join(combi)}_150arcsec_{warming}_global.npz")
    
    # Calculate frequency curve and identify 100-year event
    fq = combined_impact.calc_freq_curve()
    condition = combined_impact.at_event > fq.impact[fq.return_per > 100].iloc[1]
    event = np.where(condition)[0][0]
    
    # Select and process the 100-year event
    eai_exp = np.squeeze(np.asarray(combined_impact.imp_mat[event, :].todense()))
    combined_impact = combined_impact.select(event_ids=combined_impact.event_id[event])
    combined_impact.eai_exp = eai_exp
    
    # Save processed data
    combined_impact.write_csv(f"{OUTPUT_DIR}/100y_event/{exposure}_100y_event_compound_ordered_{'_'.join(combi)}_150arcsec_{warming}_global.csv")
    return event
def process_individual_hazards(exposure, warming, event):
    """Process individual hazard data for the identified 100-year event."""
    impacts_yearsets_ordered = {}
    for hazard in ['TC', 'RF']:
        impacts_yearsets_ordered[hazard] = Impact.from_csv(f"{OUTPUT_DIR}/yearsets/global/csv/{hazard}_{exposure}_impacts_yearsets_150arcsec_{warming}_global_caped.csv")
        impacts_yearsets_ordered[hazard].imp_mat = Impact.read_sparse_csr(f"{OUTPUT_DIR}/yearsets/global/npz/{hazard}_{exposure}_impacts_yearsets_150arcsec_{warming}_global_caped.npz")
        
        eai_exp = np.squeeze(np.asarray(impacts_yearsets_ordered[hazard].imp_mat[event, :].todense()))
        impacts_yearsets_ordered[hazard] = impacts_yearsets_ordered[hazard].select(event_ids=impacts_yearsets_ordered[hazard].event_id[event])
        impacts_yearsets_ordered[hazard].eai_exp = eai_exp
        
        impacts_yearsets_ordered[hazard].write_csv(f"{OUTPUT_DIR}/100y_event/{hazard}_{exposure}_100y_event_yearsets_150arcsec_{warming}_global.csv")

def main():
    hazards = ['TC', 'RF']
    combinations = list(itertools.combinations(hazards, 2))
    for exposure in ['pop', 'assets']:
        for warming in ['1', '2']:
            for combi in combinations:
                # Load, process, and save combined impact data
                event = load_and_process_combined_impact(exposure, combi, warming)
                # Further process individual hazards based on the 100-year event identified in the combined impact
                process_individual_hazards(exposure, warming, event)

if __name__ == "__main__":
    main()
