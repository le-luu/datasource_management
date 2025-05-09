"""
Project: Data Source Management
Created by: Le Luu

Objective: 
This project is designed to help users manage published data sources on Tableau Cloud or Server. 
It retrieves detailed information about the fields within a published data source, including formulas for calculated fields (if present). 
Additionally, it provides metadata such as the field caption, data type, and default aggregation.
This functionality is particularly useful for creating new calculations using the VizQL Data Service.

APIs Used:
- Tableau Metadata API
-VizQL Data Service

I hope you find this project useful, and I look forward to your feedback!
"""

import tableauserverclient as TSC
import requests
import json
import pandas as pd
from credentials import PAT_NAME, PAT_SECRET, SERVER_ADDRESS, SITE_ID


# Function get_luid
# To retrieve the metadata from VizQL, need the luid
# This function also retrive the formula, and the fullyQualifiedName from the data source
def get_luid(SERVER_ADDRESS, SITE_ID, PAT_NAME, PAT_SECRET):
    #Authorize with the credentials passed in
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name=PAT_NAME, personal_access_token=PAT_SECRET, site_id=SITE_ID)
    server = TSC.Server(SERVER_ADDRESS, use_server_version=True)

    #If it's authorized
    with server.auth.sign_in(tableau_auth):
        #write a GraphQL query to get the luid, name of the published datasource
        luid_response = server.metadata.query("""
            {
                publishedDatasources {
                    luid
                    name
                }
            }
        """)

        #Write a GraphQL query to get the fullyQualifiedName (which join with the fieldName in the response from VizQL later), formula, 
        # need the data source name to join with the published data source above
        field_response = server.metadata.query("""
            {
                calculatedFields {
                    fullyQualifiedName
                    formula
                    datasource {
                        name
                    }
                }
            }
        """)
        #The response is in JSON, store calculatedFields in fields, publishedDatasources to datasources variable
        fields = field_response['data']['calculatedFields']
        datasources = luid_response['data']['publishedDatasources']

    #unnested the data in the fields JSON
    fields_df = pd.json_normalize(fields)
    fields_df = fields_df.rename(columns={'datasource.name': 'datasource_name'})
    #remove the square bracketes in the fullyQualifiedName column
    fields_df['fullyQualifiedName'] = fields_df['fullyQualifiedName'].str.replace(r'[\[\]]', '', regex=True)
    #drop the datasource column
    fields_df = fields_df.drop(columns='datasource', axis=1)

    #Create an ID column based on the index for datasources_df
    datasources_df = pd.DataFrame(datasources)
    datasources_df.index += 1
    datasources_df.index.name = 'ID'

    #Join the fields_df and datasources_df together on the data source name
    merged_df = fields_df.merge(datasources_df, how='left', left_on='datasource_name', right_on='name')
    # Drop the null values in luid which are not published data source
    merged_df = merged_df.dropna(subset=['luid'])

    #print out the screent the total number of data source and the data source name with ID for user to choose
    print(f"\nThere are {len(datasources_df)} published datasources on site ===> {SERVER_ADDRESS}#/{SITE_ID}")
    print(datasources_df[['name']])

    try:
        #Ask user to choose an ID represented the data source 
        user_input = int(input("\nPlease enter the ID of the datasource you want: "))
        if user_input in datasources_df.index:
            selected_row = datasources_df.loc[user_input]
            print("\n*** Thank you! You selected: ***")
            print(f"==> Datasource Name: {selected_row['name']}")
            print(f"==> luid: {selected_row['luid']}\n")


            # Return the selected data source
            selected_df = merged_df[merged_df['luid'] == selected_row['luid']].copy()
            selected_df['datasource_name'] = selected_row['name']
            return selected_df
        else:
            print("Invalid ID.")
            return None
    except ValueError:
        print("Please enter a valid number.")
        return None

#get_token function
#This function returns the token which is required for authorization step in sending the request in VizQL
def get_token(PAT_NAME, PAT_SECRET, SERVER_ADDRESS, SITE_ID):
    url = SERVER_ADDRESS + "api/3.24/auth/signin"
    #Input the PAT info
    payload = json.dumps({
        "credentials": {
            "personalAccessTokenName": PAT_NAME,
            "personalAccessTokenSecret": PAT_SECRET,
            "site": {
                "contentUrl": SITE_ID
            }
        }
    })
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    #Send the post request and parse JSON to return the token
    response = requests.request("POST", url, headers=headers, data=payload)
    token = response.json()['credentials']['token']
    return token


#get_metadata function
#This function will return all fields in the specified data source by passing the luid and applying VizQL to read metadata
#Then joining the result with the datasource df from the get_token function to get the formula for some calculated fields
def get_metadata(token, datasource_df, SERVER_ADDRESS):
    url = SERVER_ADDRESS + "api/v1/vizql-data-service/read-metadata"

    payload = json.dumps({
        "datasource": {
            #only returns all fields with the specified luid
            "datasourceLuid": datasource_df['luid'].iloc[0]
        }
    })
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'x-tableau-auth': token
    }

    #Send the post request to the url to get the response
    response = requests.request("POST", url, headers=headers, data=payload)
    #Parse JSON
    metadata = response.json()['data']
    df = pd.DataFrame(metadata)

    #df['datasource_name'] = datasource_df['datasource_name'].iloc[0]
    #Join the metadata result from VizQL with the datasource_df on field name and fullyQualifiedName
    #I want to show all fields from the result in VizQL, so I used left join
    merged_df = df.merge(datasource_df, how='left', left_on='fieldName', right_on='fullyQualifiedName')
    #Drop some unnecessary columns and return the data frame
    merged_df = merged_df.drop(columns=['logicalTableId','datasource_name','luid','fullyQualifiedName','name'], axis=1)
    return merged_df


def main():
    while True:
        print("     ========================================================")
        print("     ====== Welcome to Published Data Source Management======")
        print("     ====== By: Le Luu                                 ======")
        print("     ========================================================\n")

        #Call functions to run the program and create a loop for user to choose another data source
        datasource_df = get_luid(SERVER_ADDRESS, SITE_ID, PAT_NAME, PAT_SECRET)
        token = get_token(PAT_NAME, PAT_SECRET, SERVER_ADDRESS, SITE_ID)

        if datasource_df is not None and not datasource_df.empty:
            df = get_metadata(token, datasource_df, SERVER_ADDRESS)
            print("             ====================================================================================")
            print(f"             ======== Metadata for the Datasource: {datasource_df['datasource_name'].iloc[0]}  =========")
            print("             ====================================================================================\n")
            print(df)
        choice = input("\nWould you like to choose another datasource? (y/n): ").strip().lower()
        if choice != 'y':
            print("Program is exiting... Good bye! Have a great day!")
            print("See you next time! ~~ Le Luu ~~")
            break


if __name__ == "__main__":
    main()
