import os
import ntpath
from src.config import get_config
from src.csv_utils import read_workbook_columns, read_workbook_data
from src.mssql_db import init_db, close, execute_sp
from src.utils import format_number, get_now_datetime
from src.scheduled_tasks_helper import execute_scheduled_tasks_sp


def process_spreadsheet_data(file, resume='', row_limit_display=100, task_id=''):
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

	filename = ntpath.basename(file)
	filepath = ntpath.dirname(file)

	start_at = 1
	if resume != '':
		start_at_result = execute_sp('MWH_FILES.MANAGE_CSV_DATA', {
			'message': 'GET_LAST_ROW',
			'PATH': filepath,
			'FILE_NAME': filename,
			'COLUMN_NAME': '',
			'COLUMN_POSITION': '',
			'ROW_NUMBER': '',
			'VALUE': ''
		}, out_arg='return_flg')
		start_at = start_at_result[0][0]['last_row']

	total = 0
	insert_count = 0
	update_count = 0
	null_count = 0
	error_count = 0

	# CSV file columns
	columns = read_workbook_columns(file)

	# CSV file rows
	rows = read_workbook_data(file)

	totals_rows = len(rows)

	if task_id:
		execute_scheduled_tasks_sp(
			'MWH.MANAGE_SCHEDULE_TASK_JOBS',
			'START_PROCESSING_SCHEDULE_TASK',
			str(task_id),
			str(totals_rows)
		)

	for row_num, row in enumerate(rows):
		curr_row = row_num + 1
		to_row = row_num + row_limit_display

		if curr_row < start_at:
			continue

		if to_row >= totals_rows:
			to_row = totals_rows

		if row_num % row_limit_display == 0:
			print(f"{get_now_datetime()}: processing rows {format_number(curr_row)} to {format_number(to_row)} of {format_number(totals_rows)}")

		for col_pos, col in enumerate(columns):
			value = row[col_pos]
			value_norm = value.lower()
			if  value_norm == 'null' or value_norm == 'PrivacySuppressed':
				processed = 3
			else:
				result = execute_sp('MWH_FILES.MANAGE_CSV_DATA', {
					'message': 'SAVE',
					'PATH': filepath,
					'FILE_NAME': filename,
					'COLUMN_NAME': col,
					'COLUMN_POSITION': str(col_pos + 1),
					'ROW_NUMBER': str(row_num + 1),
					'VALUE': value
				}, out_arg='return_flg')

				processed = result[len(result) - 1][0]['return_flg']

			total += 1

			if processed == 1:
				insert_count += 1
			elif processed == 2:
				update_count += 1
			elif processed == 3:
				null_count += 1
			else:
				error_count += 1

	if task_id:
		execute_scheduled_tasks_sp(
			'MWH.MANAGE_SCHEDULE_TASK_JOBS',
			'FINISHED_PROCESSING_SCHEDULE_TASK',
			str(task_id),
			str(total)
		)

	# Close DB connection
	close()

	print("")
	print(f"TOTAL: {format_number(total)}")
	print(f"INSERT COUNT: {format_number(insert_count)}")
	print(f"UPDATE COUNT: {format_number(update_count)}")
	print(f"NULL COUNT: {format_number(null_count)}")
	print(f"ERROR COUNT: {format_number(error_count)}")
