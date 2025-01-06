# Python Standard Library imports
import argparse
import datetime
import io
import random
import re
import time
from pathlib import Path
import requests
import pandas as pd

# CONSTANTS

# Dictionary of DTYPES, note Employee # is preserved as the index so it's not included
RSV_DF_DTYPES = {
    'Employee Name' : 'string',
    'RSV Date'      : 'datetime64[ns]',
    'RSV Type'      : 'string',
    'Avail. Days'   : 'Int64',
    'Crnt Asgmt'    : 'string',
    'Ends At'       : 'datetime64[ns]',
    'Avail At'      : 'datetime64[ns]',
    'Legal to Rept' : 'datetime64[ns]',
    'Next Off'      : 'string',
    'SC Cap'        : 'Int64',
    'SC Ratio'      : 'string',
    'FNF'           : 'string',
    'Waived'        : 'string',
    'LSR/SC/FSB'    : 'string',
    'Category'      : 'string',
    'Timestamp'     : 'datetime64[ns]'
}

# Column list to validate uploaded file
RSV_DF_FORMAT = list(RSV_DF_DTYPES.keys())

BASES_W_FLEETS = {
    'EWR':   ('320', '737', '756', '777', '787'),
    'DCA':   ('320', '737', '756', '777', '787'),
    'MCO':   ('737', ),
    'CLE':   ('737', ),
    'ORD':   ('320', '737', '756', '787'),
    'IAH':   ('320', '737', '756', '777', '787'),
    'DEN':   ('320', '737', '756', '787'),
    'LAS':   ('737', ),
    'LAX':   ('320', '737', '756', '787'),
    'SFO':   ('320', '737', '756', '777', '787'),
    'GUM':   ('737', )
}
SEATS = ('FO', 'CA')
ALL_CATS = [(b, e, s)
            for b in BASES_W_FLEETS for e in BASES_W_FLEETS[b] for s in SEATS]

# This RE extracts the SKEY from a URL.
SKEY_RE = r'.+SKEY\=(?P<skey>.{41}).*'

# We memorialize all the possible POST values that could be sent back to CCS
# here. While we don't use most of them, in the future we might so here they
# are stashed.
FULL_RSV_URL_PAYLOAD = {
    '__EVENTTARGET': '',
    '__EVENTARGUMENT': '',
    '__VIEWSTATE': '/wEPDwUJNDkzOTg0MjM1ZGQHFu8g33TJYVIq7/Ufggrgq387Mg==',
    'ctl01$mHolder$txtDate': 'date',
    'BaseCombo1': 'the_base',
    'PositionCombo1': 'the_position',
    'EquipmentCombo1': 'the_equipment',
    'ctl01$mHolder$Submit': 'Submit',
    'ctl01$EID': '*EID*=UXXXXXX-->',
    'ctl01$Carrier': '*Carrier*=UA-->',
    'ctl01$Class': '*Class*=P-->',
    'ctl01$BidPeriodsHidden': '[&quot;2024-06-30&quot;,&quot;2024-07-01&quot;,&quot;2024-07-02&quot;,&quot;2024-07-03&quot;,&quot;2024-07-04&quot;,&quot;2024-07-05&quot;,&quot;2024-07-06&quot;,&quot;2024-07-07&quot;,&quot;2024-07-08&quot;,&quot;2024-07-09&quot;,&quot;2024-07-10&quot;,&quot;2024-07-11&quot;,&quot;2024-07-12&quot;,&quot;2024-07-13&quot;,&quot;2024-07-14&quot;,&quot;2024-07-15&quot;,&quot;2024-07-16&quot;,&quot;2024-07-17&quot;,&quot;2024-07-18&quot;,&quot;2024-07-19&quot;,&quot;2024-07-20&quot;,&quot;2024-07-21&quot;,&quot;2024-07-22&quot;,&quot;2024-07-23&quot;,&quot;2024-07-24&quot;,&quot;2024-07-25&quot;,&quot;2024-07-26&quot;,&quot;2024-07-27&quot;,&quot;2024-07-28&quot;,&quot;2024-07-29&quot;,&quot;2024-08-29&quot;,&quot;2024-08-30&quot;,&quot;2024-08-31&quot;,&quot;2024-09-01&quot;,&quot;2024-09-02&quot;,&quot;2024-09-03&quot;,&quot;2024-09-04&quot;,&quot;2024-09-05&quot;,&quot;2024-09-06&quot;,&quot;2024-09-07&quot;,&quot;2024-09-08&quot;,&quot;2024-09-09&quot;,&quot;2024-09-10&quot;,&quot;2024-09-11&quot;,&quot;2024-09-12&quot;,&quot;2024-09-13&quot;,&quot;2024-09-14&quot;,&quot;2024-09-15&quot;,&quot;2024-09-16&quot;,&quot;2024-09-17&quot;,&quot;2024-09-18&quot;,&quot;2024-09-19&quot;,&quot;2024-09-20&quot;,&quot;2024-09-21&quot;,&quot;2024-09-22&quot;,&quot;2024-09-23&quot;,&quot;2024-09-24&quot;,&quot;2024-09-25&quot;,&quot;2024-09-26&quot;,&quot;2024-09-27&quot;,&quot;2024-09-28&quot;,&quot;2024-10-30&quot;,&quot;2024-10-31&quot;,&quot;2024-11-01&quot;,&quot;2024-11-02&quot;,&quot;2024-11-03&quot;,&quot;2024-11-04&quot;,&quot;2024-11-05&quot;,&quot;2024-11-06&quot;,&quot;2024-11-07&quot;,&quot;2024-11-08&quot;,&quot;2024-11-09&quot;,&quot;2024-11-10&quot;,&quot;2024-11-11&quot;,&quot;2024-11-12&quot;,&quot;2024-11-13&quot;,&quot;2024-11-14&quot;,&quot;2024-11-15&quot;,&quot;2024-11-16&quot;,&quot;2024-11-17&quot;,&quot;2024-11-18&quot;,&quot;2024-11-19&quot;,&quot;2024-11-20&quot;,&quot;2024-11-21&quot;,&quot;2024-11-22&quot;,&quot;2024-11-23&quot;,&quot;2024-11-24&quot;,&quot;2024-11-25&quot;,&quot;2024-11-26&quot;,&quot;2024-11-27&quot;,&quot;2024-11-28&quot;,&quot;2024-11-29&quot;]',
    '__VIEWSTATEGENERATOR': 'A4FAF84E'
}


def extract_datetime(text_dati, ref_date):
    """
    This function extracts a actual datetime object from the textual
    representation [text_dati] in the html. This is actually harder than
    you would think, because we don't actually know the month, or even year,
    of the date, because of calendar rollover.

    '19/15:24' --> datetime.datetime(2024, 8, 19, 15, 24)
    """
    regex = re.compile(
        r'(?P<day>[0-9]{1,2})/(?P<hour>[0-9]{2})(?P<minute>[0-9]{2})'
    )
    if mob := regex.match(str(text_dati)):
        td = mob.groupdict()
    else:
        # if no text, or not in the right format, just return None
        return

    # Integerize the td fields.
    for field in ('day', 'hour', 'minute'):
        td[field] = int(td[field])

    # Bascially, figure out if the text date is before or after the ref_date.
    # To do this, we assume that the two *dates* are within *7 days* of each
    # other. If the *day number* differs by more than *7*, we assume that's
    # because the dates they refer to are in different months and/or years.
    #
    # We store our conclusions in the td object itself.
    if abs(td['day'] - ref_date.day) < 7:
        # The easy case - the text and reference dates are in the same mth/yr
        td['year'] = ref_date.year
        td['month'] = ref_date.month
    elif td['day'] < ref_date.day:
        # For this to happen, the text date must be in the *next* month or
        # year. Think of this, remembering that 04 < 29:
        #
        # ... 29, 30, 31, 01, 02, 03, 04, 05 ...
        #      ^                       ^
        #   ref_date               text_date
        if 0 < ref_date.month < 12:
            # Same year, next month
            td['year'] = ref_date.year
            td['month'] = ref_date.month + 1
        elif ref_date.month == 12:
            # Next year, January
            td['year'] = ref_date.year + 1
            td['month'] = 1
        else:
            raise ValueError(f'Illegal month in ref_date! {ref_date}')
    elif td['day'] > ref_date.day:
        # For this to happen, the text date must be in the *previous* month or
        # year. Think of this, remembering that 04 < 29:
        #
        # ... 29, 30, 31, 01, 02, 03, 04, 05 ...
        #      ^                       ^
        #  text_date                ref_date
        if 12 >= ref_date.month > 1:
            # Same year, last month
            td['year'] = ref_date.year
            td['month'] = ref_date.month - 1
        elif ref_date.month == 1:
            # Last year, December
            td['year'] = ref_date.year - 1
            td['month'] = 12
        else:
            raise ValueError(f'Illegal month in ref_date! {ref_date}')

    # The td object should now contain all the fields needed to create a
    # proper datetime.datetime object.
    return datetime.datetime(**td)


def extract_tables(rsv_display_html, cat):
    """
    This extracts the four tables from the raw html [rsv_display_html] and
    appropriately formats them into a single dataframe which it returns.
    Note we *must* provide the Category [cat] the table is of, because that
    information is nowhere inside the tables themselves.

    The first table has no useful info. First entry of each table is the
    header, so we strip it.
    """

    # Pandas wants the HTML as a io.StringIO object, so we do that here.
    rsv_display_html = io.StringIO(initial_value=rsv_display_html)

    # Now extract and join the tables into one dataframe.
    tables = pd.read_html(rsv_display_html)
    # table 0: junk, table 1: LSR, table 2: SC, table 3: FSB
    for idx, code in zip((1, 2, 3), ('LSR', 'SC', 'FSB')):
        # 0th row is actually the header names
        tables[idx].columns = tables[idx].iloc[0]
        # 1st row to end is the data
        tables[idx] = tables[idx][1:]
        tables[idx]['LSR/SC/FSB'] = code

    # Merge the tables now that we've recorded which table the pilot
    # came from
    df = pd.concat([tables[1], tables[2], tables[3]])

    # Add BES columns and timestamp
    # df['Base'] = cat[0]
    # df['Equipment'] = cat[1]
    # df['Seat'] = cat[2]

    df['Category'] = cat[0] + cat[1] + cat[2]
    df['Timestamp'] = datetime.datetime.now()

    # Finally, return the completed DataFrame.
    return df


def extract_rsv_html(rsv_url, cat, rsv_date):
    rsv_date_string = rsv_date.strftime('%d%m%y')

    # Create payload for html POST request
    rsv_url_payload = {
        '__VIEWSTATE': '/wEPDwUJNDkzOTg0MjM1ZGQHFu8g33TJYVIq7/Ufggrgq387Mg==',
        'ctl01$mHolder$txtDate': rsv_date_string,
        'BaseCombo1': cat[0],
        'PositionCombo1': cat[2],
        'EquipmentCombo1': cat[1],
        'ctl01$mHolder$Submit': 'Submit',
        '__VIEWSTATEGENERATOR': 'A4FAF84E'
    }

    rsv_display_html = requests.post(url=rsv_url, data=rsv_url_payload,
                                     headers={}).text

    return rsv_display_html


def extract_rsv_list(rsv_url, cat, rsv_date):
    # Try to load the html 3 times, checking for errors
    # If max attempts reached, returns an empty DataFrame
    max_attempts = 3
    attempts = 1
    raw_html = ''
    while attempts <= max_attempts:
        time.sleep(random.uniform(2, 4.5))
        raw_html = extract_rsv_html(rsv_url, cat, rsv_date)
        bad_html = 'error occurred' in raw_html
        if bad_html:
            attempts += 1
            if attempts >= max_attempts:
                return pd.DataFrame()  # Return empty dataframe
        else:
            # Used to be a warning here
            break

    df = extract_tables(raw_html, cat)

    # Convert to appropriate dtypes
    df = df.replace({float('nan'): None})
    
    df['Ends At'] = df['Ends At'].apply(
        lambda x: extract_datetime(x, rsv_date))
    df['Avail At'] = df['Avail At'].apply(
        lambda x: extract_datetime(x, rsv_date))
    df['Legal to Rept'] = df['Legal to Rept'].apply(
        lambda x: extract_datetime(x, rsv_date))

    # Add Reserve Date
    df.insert(loc = 2,
              column = 'RSV Date',
              value = rsv_date)
    
    for col, dtype in RSV_DF_DTYPES.items():
        df[col] = df[col].astype(dtype=dtype)
    
    return df


def initialize_session(skey):
    # Generates the RSV URL and posts a GET request checking for a good status code
    session = requests.Session()

    rsv_url = (f'https://ccs.ual.com/CCS/ReserveAvailability.aspx?'
               f'SKEY={skey}&CMS=False')

    if (session.get(rsv_url, verify=False, headers=requests.utils.default_headers()).status_code != 200):
        raise ValueError(msg='Session is not valid!')

    return rsv_url