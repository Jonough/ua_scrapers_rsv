import streamlit as st
from datetime import datetime

import plotly.express as px
import pickle

# Local Imports
from ua_scrapers_ref import *

# Streamlit Config

st.set_page_config(
    page_title='UA Scrapers Rsv', layout='centered')  # Alternatively 'wide'
st.logo(
    image='SSCLogoLowRes.png',
    size='large')
st.title('UA Scrapers - Reserve')

# Main functions


def process_rsv(skey, cats, rsv_date):
    # For the progress bar
    l = len(cats)
    i = 0
    prog = st.progress(0)

    rsv_url = initialize_session(skey)

    df = pd.DataFrame()
    for cat in cats:
        prog.progress(i/l, f'{cat[0]}{cat[1]}{cat[2]}')
        i += 1

        r = extract_rsv_list(rsv_url, cat, rsv_date)
        if r.empty:
            st.write(
                f'Error with {cat[0]}{cat[1]}{cat[2]}: Connection error or no reserves')
            time.sleep(5)  # So user can see the error
        else:
            df = pd.concat((df, r), ignore_index=True)

    prog.progress(100, "Done!")
    # It didn't crash! Make sure the user sees this
    time.sleep(2)

    df.set_index('Employee #', inplace=True)

    return df


if ('rsv_form' not in st.session_state) and ('rsv_list' not in st.session_state):
    # Form branch
    with st.form(key='skey_getter', border=True,
                 enter_to_submit=True, clear_on_submit=False):
        st.write('Enter Data to Scrape CCS Reserve List (UPA23)')
        ccs_url = st.text_area('Enter any CCS URL:')

        # Bases multiselect with all option:
        all = st.checkbox('Select all bases')
        st.write('OR')
        selected_bases = st.multiselect('Select one or more options:',
                                        BASES_W_FLEETS)

        rsv_date = st.date_input('Enter Date')

        st.write('OR')
        st.write('Upload Reserve List from a PKL File')
        rsv_file = st.file_uploader('Choose a file')

        # On Submit
        if st.form_submit_button():
            if rsv_file is not None:
                # There is a file, try to import the reserve list
                try:
                    rsv = pd.read_pickle(rsv_file)

                    # Check to see if the imported DF is in the correct format
                    if (set(RSV_DF_FORMAT).issubset(rsv.columns)):
                        st.session_state.rsv_list = rsv
                        st.rerun()
                except:
                    st.write(
                        'Issue reading the file! Please select another file.')
            else:
                # No file, so check if the form data is valid
                # If all selected, fill in selected bases
                if (all):
                    selected_bases = list(BASES_W_FLEETS.keys())

                # Check valid skey can be extracted from URL
                if match := re.match(SKEY_RE, ccs_url):
                    # If no bases, prompt user to enter at least one
                    if not selected_bases:
                        st.write('Please select at least one base!')
                    elif rsv_date is None:
                        st.write('Please select a valid date!')
                    else:
                        # All inputs are valid, save them to a cached tuple and rerun the script
                        st.session_state.rsv_form = (match.group(
                            'skey'), selected_bases, rsv_date)
                        st.rerun()
                else:
                    st.write('Not a valid CCS URL!')
else:
    # We have form data but no rsv_list, so scrape the list
    if 'rsv_list' not in st.session_state:
        # Unpack the form data
        skey, selected_bases, rsv_date = st.session_state.rsv_form

        # Turn selected bases into a list of selected categories by filtering all categories
        selected_cats = list(
            filter(lambda c: c[0] in selected_bases, ALL_CATS))

        with st.container(border=True):
            # Only run this once per session, we don't want CCS to get angry 😡 branching should take care of this
            st.write(f'Your Session Key: {skey}')
            st.write(rsv_date.strftime('%B %d, %Y'))

            st.session_state.rsv_list = process_rsv(
                skey, selected_cats, rsv_date)

            st.rerun()
    else:
        # Main branch that displays everything
        rsv_list = st.session_state.rsv_list.copy()

        # Get the reserve date and drop the column
        rsv_date = rsv_list['RSV Date'].iloc[0].date()
        rsv_list.drop('RSV Date', axis=1, inplace=True)

        # Main Download for future use, pickle format preserves data types and handles None values appropriately
        rsv_date_string = rsv_date.strftime('%d%m%y')
        st.download_button('Download Entire Reserve List', data=pickle.dumps(st.session_state.rsv_list),
                           file_name=f'RSV_{rsv_date_string}.pkl')

        # CSV Download option
        # st.download_button('Download Entire Reserve List as CSV', data=rsv_list.to_csv(index=True),
        #                       file_name=f'TODOcsv.csv')

        # Write the reserve date
        rsv_date_string = rsv_date.strftime('%B %d, %Y')
        st.write(f'Reserve Date: {rsv_date_string}')

        # Dates to string without seconds, formatted for easier reading - this may affect correct sorting
        date_display_format = '%y-%m-%d  |  %H:%M'
        rsv_list['Ends At'] = rsv_list['Ends At'].dt.strftime(
            date_display_format)
        rsv_list['Avail At'] = rsv_list['Avail At'].dt.strftime(
            date_display_format)
        rsv_list['Legal to Rept'] = rsv_list['Legal to Rept'].dt.strftime(
            date_display_format)

        # Dropdowns for Category and Reserve Type
        cats = sorted(rsv_list['Category'].unique().tolist())
        cats.insert(0, 'ALL')
        rsv_types = rsv_list['LSR/SC/FSB'].unique().tolist()
        rsv_types.insert(0, 'ALL')
        left, right = st.columns(2)
        selected_cat = left.selectbox('Category', cats)
        rsv_type = right.selectbox('LSR/SC/FSB', rsv_types)

        # Filter categories if ALL not selected
        if (selected_cat != 'ALL'):
            rsv_list = rsv_list[rsv_list['Category'] == selected_cat]

        # Filter reserve type if ALL not selected
        if (rsv_type != 'ALL'):
            rsv_list = rsv_list[rsv_list['LSR/SC/FSB'] == rsv_type]

        st.write(rsv_list)

        # Pie Charts
        if 'rsv_charts' not in st.session_state:
            # Create the pie charts
            rsv_list = st.session_state.rsv_list.copy()
            rsv_lists_grouped = rsv_list.groupby('Category')

            # Dictionary to store charts, cat: fig
            rsv_charts = {}

            for cat, r_list in rsv_lists_grouped:
                df = r_list.groupby(
                    'LSR/SC/FSB')['LSR/SC/FSB'].count().reset_index(name='Count')
                fig = px.pie(df, values='Count', names='LSR/SC/FSB', title=cat)
                rsv_charts[cat] = fig
            
            st.session_state.rsv_charts = rsv_charts
            st.rerun()
        else:
            if (selected_cat != 'ALL'):
                st.plotly_chart(st.session_state.rsv_charts[selected_cat])