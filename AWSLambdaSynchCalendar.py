#####################################################################
####  Lambda function: funcSynchCalendar
####  Author: fluff code
####  A boto3 python handler function 
####  This lambda function is triggered whenever there is a 
####  message in the SQL Queue. It reads the event messages as input 
###   from AWS SQS and then submits the messages to the 
####  remote api call at thedoctorproctor website.
####  Subsequently the SQS message is deleted
####  Most of the static environment variables are read using import 'os' 
####  SQS queue: git-calendar-sqs.fifo 
#####################################################################

import boto3
import requests
import json
import os


#####################################################################
## fn_auth_proctor : function to do the auth for AWS api call
#####################################################################
def fn_auth_proctor ( login_url, sessiondata):
 
    response = requests.post(login_url, data = sessiondata)
    print("*"*30)
    print(response.content)
    print("*"*30)
    
    token = json.loads(response.content)['token']
    headers = { 'Authorization' : 'Bearer ' + token }
    
    print("*"*30)
    print(headers)
    print("*"*30)
    
    return headers

#####################################################################
### fn_send_msg_api: function to send message through api to website
#####################################################################
def fn_send_msg_api( url, payload, headers ):
    
    print("function fn_send_msg_api starts")
    print(headers)
    print(payload)
   
    response = requests.request("POST", url, headers=headers, data=payload)
    
    print('Response from thedoctorproctor website for api call: ')
    print(response.text.encode('utf8'))
    print("function fn_send_msg_api ends")
    #print(response)
    return 0
    
#####################################################################
### lambda_handler : main lambda handler function
#####################################################################
def lambda_handler(event, context):
    
        print("*"*30)
        print('Lambda handler function starts')
        print("*"*30)
        
        #url = "https://api.thedoctorproctor.com/api/organisations/1/ext-sessions"
        #login_url = "https://api.thedoctorproctor.com/api/auth/login"
        
        # standby instance
        url = "https://sbapi.thedoctorproctor.com/api/organisations/1/ext-sessions"
        login_url = "https://sbapi.thedoctorproctor.com/api/auth/login"
        
        #http://sbapp.thedoctorproctor.com/#/login
        
        
        sessiondata = { 'email' : 'test@gmail.com', 'password' : '1234566758' }
    
        s_aws_access_key_id = os.environ['aws_access_key_id']
        s_aws_secret_access_key = os.environ['aws_secret_access_key']
        
        
        # Create SQS client:
        #sqs = boto3.client('sqs',aws_access_key_id = 'XXXXXYUSUAKIA5XKNM3' , aws_secret_access_key = 'tughuydushTYUdTRM67868GHUYUtd7aDPkEQukIYfGDpn')
        sqs = boto3.client('sqs',aws_access_key_id = s_aws_access_key_id , aws_secret_access_key = s_awe_secret_access_key )
       
        #queue_url = 'https://sqs.us-east-1.amazonaws.com/xxxxx/git-calendar-sqs.fifo'
        queue_url = os.environ['sqs_queue_url']
        
        ## Authenticate function
        authheaders = fn_auth_proctor(login_url, sessiondata)
        
        nucount = len(event['Records'])
        print("no of SQS messages is:")
        print(nucount)
        if  nucount > 0:
                    #for message in messages['Messages']:
            for message in event['Records']:    
                        print(message['body'])
                        
                        messagebody=(message['body'])
                        receipt_handle=(message['receiptHandle'])
        
                        print(type(messagebody))
                        payload=json.loads(messagebody)
                        print("new message payload is:")
                        print(payload)
                        
                        #### call to api
                        fn_send_msg_api( url, payload, authheaders )
                        
                        
                        ###delete the message
                        response = sqs.delete_message(
                          QueueUrl=queue_url,
                          ReceiptHandle=receipt_handle
                          )
                        
                        
                        print('Received and deleted message: %s' % payload)
                        print(response)
                        
        else:
            print('Queue is empty')
                   
            print("*"*30)
            print('End of the Lambda handler function')
            print("*"*30)
               
                #break
           
        return 0            

