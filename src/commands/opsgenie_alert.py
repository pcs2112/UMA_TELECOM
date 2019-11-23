from src.config import get_config
from src.mssql_db import init_db, close, execute_sp
from src.opsgenie_helper import init_opsgenie, send_alert_for_error


def opsgenie_alert():
    config = get_config()

    db_config = {
        'DB_DRIVER': config['DB_DRIVER'],
        'DB_SERVER': config['DB_SERVER'],
        'DB_NAME': config['DB_NAME'],
        'DB_USER': config['DB_USER'],
        'DB_PASSWORD': config['DB_PASSWORD'],
        'DB_TRUSTED_CONNECTION': config['DB_TRUSTED_CONNECTION']
    }

    init_db(db_config)

    results = execute_sp('MWH.Ops_Gene_Alert_Check', {})
    if len(results) < 1 or len(results[0]) < 1:
      close()
      return

    error = results[0][0]

    init_opsgenie({
      'OPSGENIE_API_KEY': config['OPSGENIE_API_KEY'],
      'OPSGENIE_GENIE_KEY': config['OPSGENIE_GENIE_KEY'],
      'IS_PRODUCTION': config['IS_PRODUCTION']
    })

    send_alert_for_error(error)

    close()
