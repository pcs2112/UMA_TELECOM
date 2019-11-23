import os
import yaml
from src.config import get_config
from src.mssql_db import init_db, close, execute_sp
from src.utils import format_number


def process_yaml_data(file):
	if os.path.exists(file) is False:
		raise FileExistsError(f"{file} is an invalid file.")

	# Init DB connection
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

	# Get the file contents
	with open(file) as fp:
		contents = fp.read()

	# Parse the file contents
	yaml_data = yaml.load(contents)['dictionary']
	
	total = 0
	source_insert_count = 0
	source_update_count = 0
	calculate_insert_count = 0
	calculate_update_count = 0
	error_count = 0
	
	print(f"Processing ...")

	for key in yaml_data.keys():
		tmp_item = yaml_data[key]
		if 'type' not in tmp_item:
			tmp_item['type'] = 'unknown'

		if 'description' not in tmp_item:
			tmp_item['description'] = 'N/A'

		items = []
		if 'calculate' in tmp_item:
			sources = tmp_item['calculate'].split(' or ')
			for source in sources:
				items.append({
					'entry_type': 'calculate',
					'source': source,
					'type': tmp_item['type'],
					'description': tmp_item['description']
				})
		else:
			tmp_item['entry_type'] = 'source'
			items.append(tmp_item)

		for item in items:
			result = execute_sp('MWH_FILES.MANAGE_CollegeScorecard_Dictionary', {
				'message': 'SAVE',
				'DICTIONARY_ENTRY_TYPE': item['entry_type'],
				'ENTRY_NAME': key,
				'COLUMN_NAME': item['source'],
				'ENTRY_DATA_TYPE': item['type'],
				'ENTRY_DESCRIPTION': item['description']
			}, out_arg='return_flg')

			result_count = len(result)
			processed = result[result_count - 1][0]['return_flg']
			
			total += 1
			
			if processed == 1:
				source_insert_count += 1
			elif processed == 2:
				source_update_count += 1
			elif processed == 3:
				calculate_insert_count += 1
			elif processed == 4:
				calculate_update_count += 1
			else:
				error_count += 1

	# Close DB connection
	close()

	print("")
	print(f"TOTAL: {format_number(total)}")
	print(f"SOURCE INSERT COUNT: {format_number(source_insert_count)}")
	print(f"SOURCE UPDATE COUNT: {format_number(source_update_count)}")
	print(f"CALCULATE INSERT COUNT: {format_number(calculate_insert_count)}")
	print(f"CALCULATE UPDATE COUNT: {format_number(calculate_update_count)}")
	print(f"ERROR COUNT: {format_number(error_count)}")
