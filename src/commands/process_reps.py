from src.rep_helper import fetch_api_rep_data, save_rep, save_rep_role, save_rep_skill, save_rep_workgroup
from src.utils import format_number, format_date


def process_reps():
	total_processed = 0
	
	print("Processing ...")
	
	data = fetch_api_rep_data()
	
	total_count = len(data['dataList'])
	
	if total_count > 0:
		for rep in data['dataList']:
			print(f"processing record {format_number(total_processed)} of {format_number(total_count)}")
			
			rep_id = save_rep(rep)
			
			for role in rep['roles']:
				save_rep_role(rep_id, role)
				
			for skill in rep['skills']:
				save_rep_skill(rep_id, skill)
				
			for workgroup in rep['workgroups']:
				save_rep_workgroup(rep_id, workgroup)
	
	print(f"TOTAL: {format_number(total_count)}")
	print(f"TOTAL PROCESSED: {format_number(total_processed)}")
