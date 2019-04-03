from datetime import datetime


def format_number(number):
	return "{:,}".format(number)


def format_date(value, in_date_format='%Y-%m-%dT%H:%M:%S', out_date_format='%Y-%m-%d %H:%M:%S'):
	if not isinstance(value, datetime):
		value = datetime.strptime(value, in_date_format)
	return value.strftime(out_date_format)


def get_now_datetime(date_format='%Y-%m-%d %H:%M:%S'):
	return datetime.now().strftime(date_format)
