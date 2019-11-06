from constants import *
import json
import requests
import adal


def create_database(DatabaseName):
    try:
        context = adal.AuthenticationContext(AUTHENTICATION_ENDPOINT + TENANT_ID)
        token_response = context.acquire_token_with_client_credentials(RESOURCE, APPLICATION_ID,CLIENT_SECRET)
        #get access token
        access_token = token_response.get('accessToken')
        headers ={"Authorization": 'Bearer ' + access_token,'Content-Type': 'application/json', 'Accept': 'application/json'}
        #endpoint to call to create database
        endpoint='https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Sql/servers/{}/databases/{}?api-version={}'.format(SUBSCRIPTION_ID,RESOURCEGROUP_NAME,SERVER_NAME,DatabaseName,API_VERSION)
        data={"location":LOCATION}
        output=requests.put(endpoint,headers=headers,data=json.dumps(data))
        #check requests output
        if output.status_code == 202:
                print ("+++++++++++++ Cloud Data store created", output.status_code)

        else:
            status= output.json().get('error', '').get('message')
            print ("++++++ Cloud Data store creation task failed - [ %s ] " % (status))

    except Exception as err:
        print ("++++++++++++ Cloud Data store creation task failed - [ %s ] " % (err))


    


