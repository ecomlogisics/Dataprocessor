#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import re
import warnings

warnings.filterwarnings('ignore')


# In[2]:


df = pd.read_csv('History_20250313135145.csv')


# In[3]:


# in column names remove spaces and replace space between words with underscore

df.columns = df.columns.str.replace(' ', '_')


# In[4]:


# select only item id, Bill_To_Account_Number    ,Tracking_Number,3   Service                   ,,Scan_Date                 ,Status,Status_Description,Route_Code,8   Delivery_Driver_Name      ,Delivery_Address          ,Delivery_City             ,Delivery_Province         ,Delivery_Postal_Code_ZIP  ,Delivery_Country          ,Latitude  ,Longitude                 ,Client_Name

selected_columns = ['Item_ID', 'Bill_To_Account_Number', 'Tracking_Number', 'Service', 'ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss)',
                    'Status', 'Status_Description', 'Route_Code','Delivery_Driver_Name', 'Delivery_Address',
                    'Delivery_City', 'Delivery_Province', 'Delivery_Postal_Code/ZIP', 'Delivery_Country',
                    'Latitude', 'Longitude', 'Client_Name']
df_selected = df[selected_columns]


# In[5]:


def clean_city(city):
    city = re.sub(r'[^a-zA-Z0-9\s]', '', str(city)) #remove special characters
    city = city.title() #convert to sentence case
    return city

df_selected['Delivery_City'] = df_selected['Delivery_City'].apply(clean_city)


# In[6]:


#  rename ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss)  to Scan_Date
df_selected = df_selected.rename(columns={'ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss)': 'Scan_Date'})


# In[7]:


# Convert 'Scan_Date' column to datetime
df_selected['Scan_Date'] = pd.to_datetime(df_selected['Scan_Date'])


# In[8]:


# extract date part of ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss) and time part and show it

# Extract date and time parts
df_selected['Date'] = df_selected['Scan_Date'].dt.date
df_selected['Time'] = df_selected['Scan_Date'].dt.time


# In[9]:


delivered_statuses = ["DEL_VERBAL", "DEL_ASR", "DEL_SIG", "DEL_OSNR"]
ofd_scan_statuses = ["ITR_OFD", "FEDEX_ACCEPTED", "PIC_CANPAR", "PURO_ACCEPTED"]
return_statuses = ["EXC_BADADDRESS", "EXC_CONS_NA", "EXC_DMG", "EXC_MECHDELAY", "EXC_MISSING",
                   "EXC_MISSORT", "EXC_NOACCESS", "EXC_NODELATTEMPT", "EXC_REC_NA", "EXC_RECCLOSED",
                   "EXC_RECUNNDKL", "EXC_REFUSED", "EXC_UNSAFE", "EXC_WEATHER", "RET_PUR",
                   "RET_TOR", "RET_WAR", "REC_TOR"]
scansort_statuses = ["SCANSORT"]
manifested_statuses=['1']
AJTM_statuses = ["AJTM"]
lost_in_transit_statuses = ["LOST_IN_TRANSIT"]
pickup_statuses = ["PU01"]

def categorize_status(status):
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
    return "Other"  # or handle unknown statuses as needed

df_selected['Updated_Status'] = df_selected['Status'].apply(categorize_status)


# In[10]:


# select the updated status with other

df_selected[df_selected['Updated_Status'] == 'Other']


# In[11]:


# select driver name route number and number of packages (number of packages condition is updated_status=ofd scans) from zara_df datewise and also seggregate by service wise

# Group by date, service, driver, and route, then count packages with 'OFD Scans' status
result_df = df_selected[df_selected['Updated_Status'] == 'OFD Scans'].groupby(['Date', 'Delivery_Driver_Name', 'Route_Code'])['Item_ID'].nunique().reset_index()

# Rename the 'Item_ID' column to 'Number_of_Packages'
result_df = result_df.rename(columns={'Item_ID': 'Number_of_Packages'})

# Display the result
result_df.head(2)


# In[12]:


# First, group df_selected by 'Date', 'Delivery_Driver_Name', 'Route_Code' to get the corresponding 'Delivery_City'

city_df = df_selected.groupby(['Date', 'Delivery_Driver_Name', 'Route_Code'])['Delivery_City'].first().reset_index()

# Now merge this city_df with the result_df
result_df = pd.merge(result_df, city_df, on=['Date', 'Delivery_Driver_Name', 'Route_Code'], how='left')


# In[13]:


route = result_df['Route_Code'].unique()
print(route)


# In[14]:


#  Add a new column that says service, condition is route code starting with 'Z-R' service is next day and route code starting with 'Z-SD' service is Same day, route code starting with Z-M service montreal

def categorize_service(route_code):
    if route_code.startswith('YYZ-SD'):  # Check for "Same Day" first
        return 'Same Day'
    elif route_code.startswith('YYZ-'):  # Then check for "Next Day"
        return 'Next Day'
    elif route_code.startswith('YUL-'):
        return 'Montreal'
    else:
        return 'Other'

result_df['Service'] = result_df['Route_Code'].apply(categorize_service)


# In[15]:


#  add two column named as Start time and End time  in result_df for Start time take the initial scan date time in  from Time in df_selected dataframe for each driver for each date and for every route code and for end time it should be the last scan time done on that day
# Create 'Start_Time' and 'End_Time' columns
result_df['Start_Time'] = result_df.apply(lambda row: df_selected[(df_selected['Date'] == row['Date']) &
                                                                (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
                                                                (df_selected['Route_Code'] == row['Route_Code'])]['Time'].min(), axis=1)

result_df['End_Time'] = result_df.apply(lambda row: df_selected[(df_selected['Date'] == row['Date']) &
                                                              (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
                                                              (df_selected['Route_Code'] == row['Route_Code'])]['Time'].max(), axis=1)
result_df.head(2)


# In[16]:


# add a column named delivered no with condition of count of unique item id with updated status delevered into this df result_df

# Calculate 'Delivered_No' based on the condition
result_df['Delivered_No'] = result_df.apply(lambda row: df_selected[(df_selected['Date'] == row['Date']) &
                                                                    (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
                                                                    (df_selected['Route_Code'] == row['Route_Code']) &
                                                                    (df_selected['Updated_Status'] == 'Delivered')]['Item_ID'].nunique(), axis=1)


# In[17]:


# Calculate 'Confirmed_Return'
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


# In[18]:


#add a column in result_df named as Rates which hold the values like this Based on service like if service is next day rate is 2.20, id the service is same day rate is 3.5 and if the service is montral then the rate is 3 and if the delivery city is in oakville, burlington then the rate is 2.45 and keep all the decimal

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
    else:
        return 0  # Or handle other cases appropriately

result_df['Rates'] = result_df.apply(calculate_rate, axis=1)


# In[19]:


# Calculate 'Amount_to_be_paid'
result_df['Amount_to_be_paid'] = result_df['Delivered_No'] * result_df['Rates']


# In[20]:


# Create separate DataFrames for different services
next_day_df = result_df[result_df['Service'] == 'Next Day']
same_day_df = result_df[result_df['Service'] == 'Same Day']
montreal_df = result_df[result_df['Service'] == 'Montreal']


# In[21]:


same_day_df.head()


# In[22]:


with pd.ExcelWriter('output.xlsx') as writer:
    next_day_df.to_excel(writer, sheet_name='Next_Day', index=False)
    same_day_df.to_excel(writer, sheet_name='Same_Day', index=False)
    montreal_df.to_excel(writer, sheet_name='Montreal', index=False)

