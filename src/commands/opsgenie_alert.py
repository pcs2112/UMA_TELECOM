from src.config import get_config
from src.mssql_db import execute_sp
from src.opsgenie_helper import init_opsgenie, send_alert_for_error
from src.utils import log


def opsgenie_alert():
    config = get_config()

    results = execute_sp('MWH.Ops_Gene_Alert_Check', {})
    if len(results) < 1 or len(results[0]) < 1:
      log('No Alert found.')
      return

    error = results[0][0]

    init_opsgenie({
      'OPSGENIE_API_KEY': config['OPSGENIE_API_KEY'],
      'OPSGENIE_GENIE_KEY': config['OPSGENIE_GENIE_KEY'],
      'IS_PRODUCTION': config['IS_PRODUCTION']
    })

    alert = send_alert_for_error(error)
    if not alert:
      log('No OpsGenie alert sent.')
    else:
      log('OpsGenie alert sent:')
      for key in error:
        log(f'{key}: {error[key]}')

      log('')
      log('OpsGenie alert response:')
      log(f'Request ID: {alert.request_id}')
      log(f'Result: {alert.result}')
      log(f'Took: {alert.took}')
