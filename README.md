# Published Datasource Management

### Created By:
Le Luu

### Objective
The goal of this project is to retrieve the metadata from the published data sources on Tableau Cloud/ Server. 
The metadata includes the name of fields, formulas, data type, default aggregation. 
The metadata would be helpful for users to write the query in VizQL to retrieve data, create calculation from the published data source.

### Youtube Video
[![Published Datasource Management](https://img.youtube.com/vi/4NPIbE-DMJ0/0.jpg)](https://www.youtube.com/watch?v=4NPIbE-DMJ0)

### APIs
- Tableau Metadata API: https://help.tableau.com/current/api/metadata_api/en-us/reference
- VizQL Data Service: https://help.tableau.com/current/api/vizql-data-service/en-us/reference/index.html

### Features:
- List all published data sources from Tableau Cloud/ Server
- Select a published data source to return the luid and data source name
- Output the field name, field Caption, data type, default aggregation and formulas

### Output
![image](https://github.com/le-luu/datasource_management/blob/main/img/graphql_published_datasource.png)
GraphQL query from Tableau Cloud/Server to return the luid, name of the published data source

![image](https://github.com/le-luu/datasource_management/blob/main/img/graphql_calculatedfields.png)
GraphQL query from Tableau Cloud/Server to return the calculated field ID, name, fully Qualified Name, formula, data type, data source (include not published data sources)

![image](https://github.com/le-luu/datasource_management/blob/main/img/vizql_metadata.png)
The result after sending the POST request in Postman to return the metadata through VizQL

![image](https://github.com/le-luu/datasource_management/blob/main/img/result.png)
The result after running the Python script

### Instructions
- You need to install Python in your local computer
- Fork the repository and clone it to your local computer
- Open the Command Prompt (for Windows) and Terminal (for Mac), change the directory to the datasource_management
  ```
  cd datasource_management
  ```
- Install and activate the virtual environment
    ```
    pip install virtualenv
    virtualenv venv
    venv\Scripts\activate
    ```    
- Install the packages in the Command Prompt
    ```
    pip install -r requirements.txt
    ```
    It may takes a few seconds to install all packages:
    - requests
    - pandas
    - json
    - tableauserverclient
- Open the credentials.py file by the Text Editor or Python IDE and replace the info by your credential info for:
    - PAT_NAME
    - PAT_SECRET
    - SERVER_ADDRESS
    - SITE_ID
- Run the Python script by typing the command:
  ```
  python published_datasource_manager.py
  ```
