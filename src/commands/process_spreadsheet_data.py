import os
import ntpath
import datetime
from src.csv_utils import read_workbook_columns, read_workbook_data
from src.mssql_db import execute_sp
from src.utils import format_number, get_now_datetime, log
from src.scheduled_tasks_helper import execute_scheduled_tasks_sp


out_arg = 'return_flg'


def get_last_row(filepath, filename):
	start_at_result = execute_sp('MWH_FILES.MANAGE_CSV_DATA', {
		'message': 'GET_LAST_ROW',
		'PATH': filepath,
		'FILE_NAME': filename,
		'COLUMN_NAME': '',
		'COLUMN_POSITION': '',
		'ROW_NUMBER': '',
		'VALUE': '',
		'FILE_LAST_MODIFIED_DTTM': ''
	}, out_arg=out_arg)
	return start_at_result[0][0]['last_row']


def process_spreadsheet_data(file, row_limit_display=100, task_id=''):
	if os.path.exists(file) is False:
		raise FileExistsError(f"{file} is an invalid file.")

	filename = ntpath.basename(file)
	filepath = ntpath.dirname(file)
	file_last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(file))
	file_last_modified_str = file_last_modified.strftime('%Y-%m-%d %H:%M:%S')
	file_exists = False

	start_at = 1
	exists = execute_sp('MWH_FILES.MANAGE_CSV_DATA', {
		'message': 'CHECK_IF_EXISTS',
		'PATH': filepath,
		'FILE_NAME': filename,
		'COLUMN_NAME': '',
		'COLUMN_POSITION': '',
		'ROW_NUMBER': '',
		'VALUE': '',
		'FILE_LAST_MODIFIED_DTTM': file_last_modified_str
	}, out_arg=out_arg)

	if len(exists[0]) > 0 and file_last_modified.date() == exists[0][0]['last_modified_dttm'].date():
		file_exists = True
		start_at = get_last_row(filepath, filename)

	execute_scheduled_tasks_sp(
		'MWH.MANAGE_SCHEDULE_TASK_JOBS',
		'TASK_REQUEST_CHECK',
		str(task_id),
		'EXISTING FILE' if file_exists else 'NEW FILE'
	)

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
	if file_exists and start_at == totals_rows:
		if task_id:
			execute_scheduled_tasks_sp(
				'MWH.MANAGE_SCHEDULE_TASK_JOBS',
				'FINISHED_PROCESSING_SCHEDULE_TASK',
				str(task_id),
				'0'
			)

		log('File already exists. Nothing new to process.')
		return

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

		if to_row >= totals_rows:
			to_row = totals_rows

		if row_num % row_limit_display == 0:
			log(f"{get_now_datetime()}: processing rows {format_number(curr_row)} to {format_number(to_row)} of {format_number(totals_rows)}")

		if curr_row < start_at:
			continue

		for col_pos, col in enumerate(columns):
			value = row[col_pos]
			value_norm = value.lower()
			if value_norm == 'null' or value_norm == 'PrivacySuppressed':
				processed = 3
			else:
				result = execute_sp('MWH_FILES.MANAGE_CSV_DATA', {
					'message': 'SAVE',
					'PATH': filepath,
					'FILE_NAME': filename,
					'COLUMN_NAME': col,
					'COLUMN_POSITION': str(col_pos + 1),
					'ROW_NUMBER': str(row_num + 1),
					'VALUE': value,
					'FILE_LAST_MODIFIED_DTTM': file_last_modified_str
				}, out_arg=out_arg)

				processed = result[len(result) - 1][0][out_arg]

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

	log("")
	log(f"TOTAL: {format_number(total)}")
	log(f"INSERT COUNT: {format_number(insert_count)}")
	log(f"UPDATE COUNT: {format_number(update_count)}")
	log(f"NULL COUNT: {format_number(null_count)}")
	log(f"ERROR COUNT: {format_number(error_count)}")
