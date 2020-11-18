import re
import numpy as np

def clean(rows):
    """Cleans JSON file for each race. rows is a list of dictionaries.
    """
    rows = map(handle_missing, rows)
    rows = map(process_rider, rows)
    return rows

def handle_missing(row):
    """Removes the Place column from a row if result was a DNF/DNP/DQ.
    """
    if any(row[col] == 1 for col in ['IsDnf', 'IsDNP', 'IsDQ']):
        row.pop('Place')
    return row

def process_rider(row):
    """Does preprocessing related to each individual rider:
    - Combines FirstName and LastName
    - Removes row (replaces with empty dict) if racer has a missing name or
        the name contains digits
    - Consolidates age columns
    """
    # Missing names - there may be more!
    if (row['RacerID'] in [3288, 61706, 832, 351]) \
        or (not row['FirstName']) \
        or (row['FirstName'] == 'Unknown') \
        or (re.search(r'[\d]', row['FirstName'])) \
        or (re.search(r'[\d]', row['LastName'])):
        return {}

    # Combine names
    row['Name'] = ' '.join([row['FirstName'], row['LastName']])

    # Combine age
    row['Age'] = max(row['CalculatedAge'] or 0, row['ReportedAge'] or 0)

    return row
