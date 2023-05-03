from bs4 import BeautifulSoup
import cx_Oracle
import pandas as pd
import numpy as np

path = 'C:\\Users\\anban39\\......XML_Files\\'
idx = 1

#Creating a pandas dataframe that would store all the partner id and del incicator
Payload_XML_df = pd.DataFrame(columns=['P_ID', 'Del_Indicator'])

# Loop through all the 27 files
for i in range(1, 28):
    # Open the XML file
    #Added encoding = 'utf-8' as got an error : UnicodeDecodeError while reading the XMLs
    with open(f"{path}CODA_{i}.xml", 'r', encoding='utf-8') as file:
        xml = file.read()

    # Create BeautifulSoup object
    soup = BeautifulSoup(xml, 'xml')

    # Find all the 'ts:item' elements in the XML file
    items = soup.find_all('ts:item')

    # Print the values of <ts:PARTNER_ID> and <ts:DELETE_ID> Set as 'X'
    for item in items:
        # Find the ts:PARTNER_ID and ts:DELETE_ID elements
        partner_id_tag = item.find('ts:PARTNER_ID')
        delete_id_tag = item.find('ts:DELETE_ID')

        # If the delete_id_tag exists and has text value "X", print the partner_id_tag value
        if delete_id_tag and delete_id_tag.text == 'X':
            print(f"{idx}. Partner ID: {partner_id_tag.text}, DELETE_ID: X")
            Payload_XML_df.loc[len(Payload_XML_df)] = [partner_id_tag.text, delete_id_tag.text]
            idx = idx+1
        # If the delete_id_tag exists and is empty, print partner_id with DELETE_ID = None
        elif delete_id_tag is not None and not delete_id_tag.text:
            print(f"{idx}. Partner ID: {partner_id_tag.text}, DELETE_ID: None")
            Payload_XML_df.loc[len(Payload_XML_df)] = [partner_id_tag.text, None]
            idx += 1



Payload_XML_df.to_csv('Payload_XML_to_CSV.csv')

#Extracting PartnerID details from Anders sheet and querying the same in PSP Table
# Connect to the database
conStr = 'table_name/table_password@HostName/ServiceName'
connection = cx_Oracle.connect(conStr)

# Create a cursor
cursor = connection.cursor()
print(cursor)

#Read Anders Partner_ids
p_id_list = pd.read_csv('Anders_PartnerId_List.csv')

# create a list of partner IDs wrapped in quotes
partner_ids = ["'{}'".format(p) for p in p_id_list['Ext.PartnerNo.']]

# join the partner IDs with commas
part_ids = ",".join(partner_ids)

# Execute a query
sqlquery = """select partner_id,seq_id, delete_id from business_partner_t
where seq_id in (
    select max(seq_id) from business_partner_t
    where partner_id in (
        {}
    )
    group by partner_id
)""".format(part_ids)

cursor.execute(sqlquery)

# Fetch the results
PSP_table_df = pd.DataFrame(cursor.fetchall())

#Rename column names
PSP_table_df.rename(columns={0:'C_Partner_ID', 1:'CUSIS_Seq_Id',2:'CUSIS_Deletion_Indicator'},inplace=True)

PSP_table_df.to_csv('PSP_table_df.csv')
# Print the results
print(PSP_table_df)

#Merging Payload_XML_df to PSP_table_df(Left Join)
final_PSP_df = pd.merge(left=PSP_table_df, right=Payload_XML_df,left_on='C_Partner_ID',
                        right_on='P_ID',
                        how='left')

print(final_PSP_df)

final_PSP_df.to_csv('Final_PSP_sheet.csv')
