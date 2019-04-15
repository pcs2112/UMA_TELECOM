from src.mssql_db import init_db, close
from src.config import get_config
from src.rep_helper import (
    fetch_api_rep_data, start_process, stop_process, save_rep, save_rep_role, save_rep_skill, save_rep_workgroup
)
from src.utils import format_number


def process_reps():
    config = get_config()
    
    db_config = {
        'DB_SERVER': config['DB_SERVER'],
        'DB_NAME': config['DB_NAME'],
        'DB_USER': config['DB_USER'],
        'DB_PASSWORD': config['DB_PASSWORD'],
        'DB_DRIVER': config['DB_DRIVER'],
        'DB_TRUSTED_CONNECTION': config['DB_TRUSTED_CONNECTION']
    }
    
    init_db(db_config)
    
    total_processed = 0
    
    print("Processing ...")
    
    master_load_id = start_process()
    if not master_load_id:
        print("Processing halted. Master load id not returned.")
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
    
    print(f"TOTAL ROWS PROCESSED: {format_number(summary['return_value'])}")
    print(f"TOTAL NEW REPS: {format_number(summary['new_rep_count'])}")
    print(f"TOTAL ROWS REMOVED: {format_number(summary['removed_count'])}")
    print(f"MASTER LOAD HISTORY ID: {master_load_id}")
    
    close()
