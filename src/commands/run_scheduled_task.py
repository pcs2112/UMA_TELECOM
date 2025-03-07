import os
import requests
import zipfile
import shutil
import datetime
import time
from src.config import get_config
from src.mssql_db import execute_sp
from src.scheduled_tasks_helper import execute_scheduled_tasks_sp, get_next
from src.utils import log
from .process_spreadsheet_data import process_spreadsheet_data
from .process_yaml_data import process_yaml_data

config = get_config()

ALL_DATA_URL = 'https://ed-public-download.app.cloud.gov/downloads/CollegeScorecard_Raw_Data.zip'
LATEST_COHORTS_DATA_URL = 'https://ed-public-download.app.cloud.gov/downloads/Most-Recent-Cohorts-All-Data-Elements.csv'
LATEST_FIELD_OF_STUDY_DATA_URL = 'https://ed-public-download.app.cloud.gov/downloads/Most-Recent-Field-Data-Elements.csv'
LOCK_FILE = os.path.join(config['TMP_DIR'], 'run_scheduled_task.lock')


def write_lock_file(task_id):
    remove_lock_file()
    f = open(LOCK_FILE, 'w')
    f.write(str(task_id))
    f.close()


def remove_lock_file():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)


def get_current_task_id():
    if not os.path.exists(LOCK_FILE):
        return None

    f = open(LOCK_FILE, 'r')
    task_id = f.read()
    f.close()
    return task_id


def run_scheduled_task():
    current_task_id = get_current_task_id()
    if current_task_id:
        log(f'Busy. Running task #{current_task_id}.')
        return

    log('Getting the next task to be executed...')
    next_task = get_next()
    if not next_task:
        log('0 scheduled tasks to run.')
        return

    task_id = str(next_task['id'])

    # Write lock file
    write_lock_file(task_id)

    # Normalize file names
    scheduled_url = next_task['ftp_site']
    scheduled_basename = scheduled_url[scheduled_url.rfind('/')+1:]
    scheduled_basename, scheduled_file_extension = os.path.splitext(scheduled_basename)
    scheduled_file_extension = scheduled_file_extension.lower()
    scheduled_basename = f'{scheduled_basename.upper()}{scheduled_file_extension}'

    # Mark task as running
    log(f'Running task #{task_id}...')

    execute_scheduled_tasks_sp(
        'MWH.MANAGE_SCHEDULE_TASK_JOBS',
        'RUNNING_NEXT_SCHEDULE_TASK',
        task_id,
        scheduled_basename
    )

    tmp_dir = os.path.join(config['TMP_DIR'], task_id)

    log('Emptying the tmp dir...')
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.mkdir(tmp_dir)

    log('The tmp dir is empty...')

    if next_task['python_job'] == 'MOST RECENT COLLEGE SCORECARD COHORTS DATA':
        download_url = LATEST_COHORTS_DATA_URL
        download_basename = download_url[download_url.rfind('/')+1:]
    elif next_task['python_job'] == 'MOST RECENT COLLEGE SCORECARD FIELDS OF STUDY':
        download_url = LATEST_FIELD_OF_STUDY_DATA_URL
        download_basename = download_url[download_url.rfind('/')+1:]
    elif '.csv' in scheduled_file_extension or '.yaml' in scheduled_basename:
        download_url = ALL_DATA_URL
        download_basename = download_url[download_url.rfind('/')+1:]
    else:
        log('Exiting because and invalid file name.')
        error_exit()
        return

    download_filename = os.path.join(tmp_dir, download_basename)

    log(f'Downloading {download_url}...')
    res = requests.get(download_url)
    with open(download_filename, 'wb') as f:
        f.write(res.content)
    log(f'Finished downloading file {download_filename}...')

    if '.zip' in download_basename:
        log(f'Extracting files in {tmp_dir}...')
        with zipfile.ZipFile(download_filename, 'r') as zip_ref:
            zip_ref.extractall(tmp_dir)
            for f in zip_ref.infolist():
                date_time = time.mktime(f.date_time + (0, 0, -1))
                os.utime(os.path.join(tmp_dir, f.filename), (date_time, date_time))

        log(f'Finished Extracting files.')

    if '.zip' in download_basename:
        downloaded_filename = os.path.join(tmp_dir, download_basename.replace('.zip', ''), scheduled_basename)
        
    else:
        downloaded_filename = os.path.join(tmp_dir, download_basename)
        new_downloaded_filename = os.path.join(tmp_dir, scheduled_basename)
        shutil.copyfile(downloaded_filename, new_downloaded_filename) 
        downloaded_filename = new_downloaded_filename
        last_modified = datetime.datetime.timestamp(
            datetime.datetime.strptime(res.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
        )
        os.utime(downloaded_filename, (last_modified, last_modified))

    if os.path.exists(downloaded_filename):
        # Mark task download as finished
        execute_scheduled_tasks_sp(
            'MWH.MANAGE_SCHEDULE_TASK_JOBS',
            'FINISHED_FTP_SAVED_FILE_SCHEDULE_TASK',
            task_id,
            scheduled_basename,
            str(os.path.getsize(downloaded_filename))
        )
    else:
        log('Exiting because the file was not found.')
        error_exit()
        return

    if '.csv' in scheduled_basename:
        process_spreadsheet_data(downloaded_filename, task_id=task_id)
    elif '.yaml' in scheduled_basename:
        process_yaml_data(downloaded_filename, task_id=task_id)

    # Remove the temp dir
    shutil.rmtree(tmp_dir, ignore_errors=True)

    # Remove the lock file
    remove_lock_file()


def error_exit():
    remove_lock_file()
