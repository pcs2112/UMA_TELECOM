from src.rep_helper import (
    fetch_api_rep_data, start_process, stop_process, save_rep, save_rep_role, save_rep_skill, save_rep_workgroup
)
from src.utils import format_number, log


def process_reps():
    total_processed = 0
    
    log("Processing ...")
    
    master_load_id = start_process()
    if not master_load_id:
        log("Processing halted. Master load id not returned.")
        exit()
    
    data = fetch_api_rep_data()
    
    total_count = len(data['dataList'])
    
    if total_count > 0:
        for rep in data['dataList']:
            rep_id = save_rep(rep, master_load_id)
            
            if rep_id:
                for role in rep['roles']:
                    save_rep_role(rep_id, role, master_load_id)
                
                for skill in rep['skills']:
                    save_rep_skill(rep_id, skill, master_load_id)
                
                for workgroup in rep['workgroups']:
                    save_rep_workgroup(rep_id, workgroup, master_load_id)
                
                total_processed += 1
    
    summary = stop_process(master_load_id)
    
    log(f"TOTAL ROWS PROCESSED: {format_number(summary['return_value'])}")
    log(f"TOTAL NEW REPS: {format_number(summary['new_rep_count'])}")
    log(f"TOTAL ROWS REMOVED: {format_number(summary['removed_count'])}")
    log(f"MASTER LOAD HISTORY ID: {master_load_id}")
