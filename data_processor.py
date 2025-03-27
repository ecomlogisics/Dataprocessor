
import pandas as pd
import re
from datetime import datetime
import warnings
import io

# Suppress warnings
warnings.filterwarnings('ignore')

def clean_text(text):
    if isinstance(text, str):
        text = re.sub(r"[^a-zA-Z0-9\s]", "", text)  # Remove special characters
        text = text.strip()
        text = ' '.join(word.capitalize() for word in text.split())  # Convert to sentence case
    return text

def categorize_status(status):
    delivered_statuses = ["DEL_VERBAL", "DEL_ASR", "DEL_SIG", "DEL_OSNR"]
    ofd_scan_statuses = ["ITR_OFD", "FEDEX_ACCEPTED", "PIC_CANPAR", "PURO_ACCEPTED"]
    return_statuses = ["EXC_BADADDRESS", "EXC_CONS_NA", "EXC_DMG", "EXC_MECHDELAY", "EXC_MISSING",
                      "EXC_MISSORT", "EXC_NOACCESS", "EXC_NODELATTEMPT", "EXC_REC_NA", "EXC_RECCLOSED",
                      "EXC_RECUNNDKL", "EXC_REFUSED", "EXC_UNSAFE", "EXC_WEATHER", "RET_PUR",
                      "RET_TOR", "RET_WAR", "REC_TOR"]
    scansort_statuses = ["SCANSORT"]
    manifested_statuses = ['1']
    AJTM_statuses = ["AJTM"]
    lost_in_transit_statuses = ["LOST_IN_TRANSIT"]
    pickup_statuses = ["PU01"]

    if status in delivered_statuses:
        return "Delivered"
    elif status in ofd_scan_statuses:
        return "OFD Scans"
    elif status in return_statuses:
        return "Return"
    elif status in scansort_statuses:
        return "Scansort"
    elif status in lost_in_transit_statuses:
        return "Lost in Transit"
    elif status in pickup_statuses:
        return "Pickup"
    elif status in AJTM_statuses:
        return "AJTM"
    elif status in manifested_statuses:
        return "Manifested"
    else:
        return "Other"

def categorize_service(route_code):
    if isinstance(route_code, str):
        if route_code.startswith('YYZ-SD'):
            return 'Same Day'
        elif route_code.startswith('YYZ-'):
            return 'Next Day'
        elif route_code.startswith('YUL-'):
            return 'Montreal'
    return 'Other'

def calculate_rate(row):
    service = row['Service']
    city = row['Delivery_City']
    if service == 'Next Day':
        if city in ['Oakville', 'Burlington']:
            return 2.45
        else:
            return 2.20
    elif service == 'Same Day':
        return 3.5
    elif service == 'Montreal':
        return 3
    return 0.0

def process_dispatch_data(df):
    # Clean column names
    df.columns = df.columns.str.replace(' ', '_')

    # Select required columns
    selected_columns = ['Item_ID', 'Bill_To_Account_Number', 'Tracking_Number', 'Service', 
                       'ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss)', 'Status', 'Status_Description', 
                       'Route_Code', 'Ship_To_Name', 'Ship_To_Address', 'Ship_To_Address_2', 
                       'Ship_To_City', 'Ship_To_State/Province', 'Ship_To_Postal_Code/ZIP', 
                       'Ship_To_Country', 'Delivery_Driver_Name', 'Delivery_Address',
                       'Delivery_City', 'Delivery_Province', 'Delivery_Postal_Code/ZIP', 
                       'Delivery_Country', 'Latitude', 'Longitude', 'Client_Name']

    df_selected = df[selected_columns].copy()

    # Clean text in relevant columns
    columns_to_clean = ['Ship_To_Name', 'Ship_To_Address', 'Ship_To_Address_2', 'Ship_To_City',
                       'Ship_To_State/Province', 'Ship_To_Postal_Code/ZIP', 'Ship_To_Country', 
                       'Delivery_City']
    
    for col in columns_to_clean:
        df_selected[col] = df_selected[col].apply(clean_text)

    # Create full address
    df_selected['Ship_To_Full_Address'] = df_selected['Ship_To_Name'].astype(str) + ', ' + \
                                        df_selected['Ship_To_Address'].astype(str) + ', ' + \
                                        df_selected['Ship_To_Address_2'].astype(str) + ', ' + \
                                        df_selected['Ship_To_City'].astype(str) + ', ' + \
                                        df_selected['Ship_To_State/Province'].astype(str) + ', ' + \
                                        df_selected['Ship_To_Postal_Code/ZIP'].astype(str) + ', ' + \
                                        df_selected['Ship_To_Country'].astype(str)

    # Process dates and times
    df_selected = df_selected.rename(columns={'ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss)': 'Scan_Date'})
    df_selected['Scan_Date'] = pd.to_datetime(df_selected['Scan_Date'])
    df_selected['Date'] = df_selected['Scan_Date'].dt.date
    df_selected['Time'] = df_selected['Scan_Date'].dt.time

    # Categorize status
    df_selected['Updated_Status'] = df_selected['Status'].apply(categorize_status)

    # Create base result DataFrame
    result_df = df_selected[df_selected['Updated_Status'] == 'OFD Scans'].groupby(
        ['Date', 'Delivery_Driver_Name', 'Route_Code'])['Item_ID'].nunique().reset_index()
    result_df = result_df.rename(columns={'Item_ID': 'Number_of_Packages'})

    # Add number of stops
    stops_df = df_selected.groupby(['Date', 'Delivery_Driver_Name', 'Route_Code'])['Ship_To_Full_Address'].nunique().reset_index(name='Number_of_Stops')
    result_df = pd.merge(result_df, stops_df, on=['Date', 'Delivery_Driver_Name', 'Route_Code'], how='left')

    # Add city information
    city_df = df_selected.groupby(['Date', 'Delivery_Driver_Name', 'Route_Code'])['Delivery_City'].first().reset_index()
    result_df = pd.merge(result_df, city_df, on=['Date', 'Delivery_Driver_Name', 'Route_Code'], how='left')

    # Add service categorization
    result_df['Service'] = result_df['Route_Code'].apply(categorize_service)

    # Add timing information
    result_df['Start_Time'] = result_df.apply(lambda row: df_selected[
        (df_selected['Date'] == row['Date']) &
        (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
        (df_selected['Route_Code'] == row['Route_Code'])]['Time'].min(), axis=1)

    result_df['End_Time'] = result_df.apply(lambda row: df_selected[
        (df_selected['Date'] == row['Date']) &
        (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
        (df_selected['Route_Code'] == row['Route_Code'])]['Time'].max(), axis=1)

    # Add delivery and return metrics
    result_df['Delivered_No'] = result_df.apply(lambda row: df_selected[
        (df_selected['Date'] == row['Date']) &
        (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
        (df_selected['Route_Code'] == row['Route_Code']) &
        (df_selected['Updated_Status'] == 'Delivered')]['Item_ID'].nunique(), axis=1)

    # Process route mismatches
    ofd_df = df_selected[df_selected['Updated_Status'] == 'OFD Scans'][['Item_ID', 'Date', 'Route_Code', 'Delivery_Driver_Name']]
    ofd_df = ofd_df.rename(columns={'Route_Code': 'OFD_Route', 'Delivery_Driver_Name': 'OFD_Driver'})

    delivered_df = df_selected[df_selected['Updated_Status'] == 'Delivered'][['Item_ID', 'Date', 'Route_Code', 'Delivery_Driver_Name']]
    delivered_df = delivered_df.rename(columns={'Route_Code': 'Delivery_Route', 'Delivery_Driver_Name': 'Delivery_Driver'})

    merged_df = pd.merge(ofd_df, delivered_df, on=['Item_ID', 'Date'], how='inner')
    wrong_ofd_df = merged_df[merged_df['OFD_Route'] != merged_df['Delivery_Route']]
    correct_ofd_df = wrong_ofd_df[wrong_ofd_df['OFD_Driver'] == wrong_ofd_df['Delivery_Driver']]

    mismatch_count_df = correct_ofd_df.groupby(['Date','OFD_Driver','OFD_Route','Delivery_Route'])['Item_ID'].count().reset_index()
    mismatch_count_df = mismatch_count_df.rename(columns={
        'Item_ID': 'Mismatch_Count',
        'OFD_Driver': 'Delivery_Driver_Name',
        'OFD_Route': 'Route_Code'
    })

    # Merge mismatch information
    result_df = pd.merge(
        result_df,
        mismatch_count_df[['Date', 'Delivery_Driver_Name', 'Delivery_Route', 'Mismatch_Count']],
        on=['Date', 'Delivery_Driver_Name'],
        how='left'
    )
    
    result_df['Mismatch_Count'] = result_df['Mismatch_Count'].fillna(0).astype(int)
    result_df = result_df.rename(columns={'Delivery_Route': 'Mismatch_Route'})

    # Calculate confirmed returns
    result_df['Confirmed_Return'] = result_df.apply(lambda row: df_selected[
        (df_selected['Date'] == row['Date']) &
        (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
        (df_selected['Route_Code'] == row['Route_Code']) &
        (df_selected['Updated_Status'] == 'Return') &
        (~df_selected['Item_ID'].isin(df_selected[
            (df_selected['Date'] == row['Date']) &
            (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
            (df_selected['Route_Code'] == row['Route_Code']) &
            (df_selected['Updated_Status'] == 'Delivered')
        ]['Item_ID']))
    ]['Item_ID'].nunique(), axis=1)

    # Calculate rates and amounts
    result_df['Rates'] = result_df.apply(calculate_rate, axis=1)
    result_df['Amount_to_be_paid'] = (result_df['Delivered_No'] + result_df['Mismatch_Count']) * result_df['Rates']

    # Split into service-specific DataFrames
    return (
        result_df[result_df['Service'] == 'Next Day'],
        result_df[result_df['Service'] == 'Same Day'],
        result_df[result_df['Service'] == 'Montreal']
    )

def create_excel_report(next_day_df, same_day_df, montreal_df):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        next_day_df.to_excel(writer, sheet_name='Next_Day', index=False)
        same_day_df.to_excel(writer, sheet_name='Same_Day', index=False)
        montreal_df.to_excel(writer, sheet_name='Montreal', index=False)
    buffer.seek(0)
    return buffer
