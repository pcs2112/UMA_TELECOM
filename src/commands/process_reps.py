from src.mssql_connection import init_db
from src.config import get_config
from src.rep_helper import fetch_api_rep_data, save_rep, save_rep_role, save_rep_skill, save_rep_workgroup
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
	
	data = fetch_api_rep_data()
	
	total_count = len(data['dataList'])
	
	if total_count > 0:
		for rep in data['dataList']:
			print(f"processing record {format_number(total_processed)} of {format_number(total_count)}")
			
			rep_id = save_rep(rep)
			
			if rep_id:
				for role in rep['roles']:
					save_rep_role(rep_id, role)
					
				for skill in rep['skills']:
					save_rep_skill(rep_id, skill)
					
				for workgroup in rep['workgroups']:
					save_rep_workgroup(rep_id, workgroup)
					
				total_processed += 1
	
	print(f"TOTAL: {format_number(total_count)}")
	print(f"TOTAL REPS PROCESSED: {format_number(total_processed)}")
