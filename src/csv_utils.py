import csv


def read_workbook_columns(wb):
    """ Returns a list of columns for the specified csv workbook. """
    line_count = 0
    header_columns = []
    with open(wb, mode='r', encoding='utf-8-sig') as fh:
        csv_reader = csv.reader(fh, delimiter=',')
        for row in csv_reader:
            if line_count == 0:
                header_columns = row
                break
    
    return header_columns


def read_workbook_data(wb):
    """
    Returns the specified csv workbook's data as an array of objects
    using the header columns as the keys.
    """
    line_count = 0
    header_columns = []
    data = []
    
    with open(wb, mode='r', encoding='utf-8-sig') as fh:
        csv_reader = csv.reader(fh, delimiter=',')
        for row in csv_reader:
            if line_count == 0:
                header_columns = row
                line_count += 1
                continue
            
            item = []
            for col in range(len(header_columns)):
                value = row[col].strip()
                item.append(value)
            
            data.append(item)
            line_count += 1
    
    return data
