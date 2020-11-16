import re
import numpy as np


def clean(df):
    '''
    Cleans dataframe for each race
    '''
    # Drop redundant and unneeded columns
    columns = [str(i) for i in range(28)] + ['RaceCategoryID',
                                             'OffTheFront',
                                             'OffTheBack',
                                             'GroupSprintPlace',
                                             'FieldSprintPlace',
                                             'License',
                                             'Starters',
                                             'FinishPhotoUrl',
                                             'MetaDataUrl',
                                             'ResultID',
                                             'RacerCount'
                                             ]
    df = df.drop(columns=columns)

    df = handle_missing(df)
    df = process_rider(df)
    df = process_team(df)
    df = process_time(df)
    df = sort_columns(df)
    df = df.drop_duplicates(['RaceCategoryName', 'RacerID'])
    df.groupby(['RaceCategoryName']).apply(check_unique_placings)
    df.groupby(['RaceCategoryName']).apply(check_placing_order)

    return df


def check_placing_order(df):
    """Checks that the numerical values for place are in ascending order."""
    assert df['Place'].dropna().is_monotonic_increasing

def check_unique_placings(df):
    """Checks to make sure all placings are unique. Sometimes results are
       listed as ties, and sometimes people are duplicated or in the wrong
       spot."""
    if not df['Place'].dropna().is_unique:
        print('Places not unique!', df['Place'])

def handle_missing(df):
    '''
    Records DNF/DNP/DQ as nans in the 'Place' column
    '''
    df.loc[df['IsDnf'] == 1, 'Place'] = np.nan
    df.loc[df['IsDNP'] == 1, 'Place'] = np.nan
    df.loc[df['IsDQ'] == 1, 'Place'] = np.nan
    return df.drop(columns=['IsDnf', 'IsDNP', 'IsDQ'])


def process_rider(df):
    '''
    Does preprocessing related to each individual rider:
    - Combines FirstName and LastName
    - Drops racers with missing names or names containing digits (bad data)
    - Consolidates age columns
    '''
    # Missing names - there may be more!
    df = df[~df['RacerID'].isin([3288, 61706, 832, 351])
            ].dropna(subset=['FirstName', 'LastName'])

    # Combine FirstName, Lastname and check for digits
    df = df.assign(Name=df['FirstName'] + ' ' + df['LastName'])
    df = df.drop(columns=['FirstName', 'LastName'])
    df = df[~df['Name'].str.contains(r'[\d]')]
    df = df[df['Name'] != 'Unknown Rider']

    age_cols = ['CalculatedAge', 'ReportedAge']
    df.loc[:, 'Age'] = df[age_cols].fillna(0).max(axis=1).replace(0, np.nan)
    df = df.drop(columns=age_cols)
    return df


def process_team(df):
    '''
    Does preprocessing related to each team:
    - Replaces missing team names with nans
    - Does not yet account for teams that may appear under multiple names
    '''
    bad_names = ['', 'unattached', '0']
    df.loc[df['TeamName'].str.lower().isin(bad_names), 'TeamName'] = np.nan
    return df


def process_time(df):
    '''
    Preprocesses race time
    '''
    return df


def sort_columns(df):
    '''
    Order and set datatypes for columns
    '''
    df = df.astype({'Place': float, 'Category': float})
    df = df[['Place', 'RaceTime', 'Name', 'Age', 'Category', 'RacerID',
             'TeamID', 'TeamName', 'RaceName', 'RaceCategoryName']]
    return df
