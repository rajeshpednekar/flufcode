#!/usr/bin/python
import os
import datetime
import boto3
import mysql.connector


###################################################
### gitcalendar_synch.py (fluff code)
### a Python procedure using boto3 to write to AWS SQS Queue 
### all the events by reading from a local MYSQL audit table 
### NOTE: Redundant use of two cursors, can be improved further.
### 
#####################################################

##################
# Mysql connection
##################
config = {
  'user': 'XXXX',
  'password': 'XXXXXX',
  'host': '127.0.0.1',
  'database': 'wp900',
  'raise_on_warnings': True
}

###################
## Main try block
###################
try:

     print("function: Synch calendar starts.")
     now = datetime.datetime.now()

     sqs = boto3.resource('sqs')
     queue = sqs.get_queue_by_name(QueueName='git-calendar-sqs.fifo')
     queueUrl = 'https://sqs.us-east-1.amazonaws.com/xxxxxx/git-calendar-sqs.fifo'

     cnx = mysql.connector.connect(**config)

     cursor_orig = cnx.cursor()

     orig_query1 = """ ( SELECT 
                  booking_id,
                  modification_date,
                  event_ts,
                  booking_date,
                 CONCAT( col_1 ,col_2, col_3, col_4, col_5, col_6) as json_val 
                 FROM 
                 ( 
                  SELECT  
                      booking_id,
                      modification_date,
                      event_ts,
                      booking_date,
                      #CAST(CONCAT('{"exam_id" : "', ifnull(title,''), '" ,') AS CHAR ) as col_1, 
                      CAST(CONCAT('{"exam_id" : "', "112", '" ,') AS CHAR ) as col_1, 
                      CAST(CONCAT('"first_name" : "', ifnull(firstname,''), '" ,') AS CHAR )  as col_2, 
                      CAST(CONCAT('"last_name" : "', ifnull(lastname,''), '" ,') AS CHAR )  as col_3, 
                      CAST(CONCAT('"student_email" : "', ifnull(emailaddress,''), '" ,') AS CHAR )  as col_4, 
                      CAST(CONCAT('"exam_date_time" : "', ifnull(booking_date,''), '" ,') AS CHAR )  as col_5, 
                      #CAST(CONCAT('"timezone" : "', ifnull(timerange,''), '" }') AS CHAR )  as col_6 
                      CAST(CONCAT('"timezone" : "', "America/New_York", '" }') AS CHAR )  as col_6 
                    FROM 
                  wp900.wp7c_booking_audit )  
                json_tbl 
                  WHERE 1 
                ) """


     cursor1 = cnx.cursor()
     distinct_query1 = """( SELECT distinct 
                     A.booking_id,
                     A.modification_date,
                     A.event_ts,
                     A.booking_date,
                   CONCAT( col_1 ,col_2, col_3, col_4, col_5, col_6) as json_val 
                FROM 
                  (  
                    SELECT  
                      booking_id,
                      modification_date,
                      event_ts,
                      booking_date,
                      #CAST(CONCAT('{"exam_id" : "', ifnull(title,''), '" ,') AS CHAR ) as col_1, 
                      CAST(CONCAT('{"exam_id" : "', "112", '" ,') AS CHAR ) as col_1, 
                      CAST(CONCAT('"first_name" : "', ifnull(firstname,''), '" ,') AS CHAR )  as col_2, 
                      CAST(CONCAT('"last_name" : "', ifnull(lastname,''), '" ,') AS CHAR )  as col_3, 
                      CAST(CONCAT('"student_email" : "', ifnull(emailaddress,''), '" ,') AS CHAR )  as col_4, 
                      CAST(CONCAT('"exam_date_time" : "', ifnull(booking_date,''), '" ,') AS CHAR )  as col_5, 
                      #CAST(CONCAT('"timezone" : "', ifnull(timerange,''), '" }') AS CHAR )  as col_6 
                      CAST(CONCAT('"timezone" : "', "America/New_York", '" }') AS CHAR )  as col_6 
                    FROM 
                  wp900.wp7c_booking_audit ) A ,
                  ( Select booking_id, 
                           min(booking_date) booking_date, 
                           min(event_ts) event_ts 
                   from 
                      wp900.wp7c_booking_audit
                     group by booking_id
                   ) as min_book 
          where min_book.booking_id = A.booking_id
          and min_book.booking_date = A.booking_date
          and min_book.event_ts = A.event_ts 
           ) """


     #hire_end = datetime.date(1999, 12, 31)
     #cursor.execute(query, (hire_start, hire_end))


     del_query = """DELETE FROM wp900.wp7c_booking_audit
          where booking_id =%s
             and modification_date = %s
             and event_ts = %s 
             and booking_date = %s """


     #############################
     ## open the orig cursor first.
     #############################
     cursor_orig.execute(orig_query1)
     rowcnt_orig = cursor_orig.fetchall()

     ###########################
     ## open the distinct cursor 
     ###########################
     cursor1.execute(distinct_query1)

     rowcnt = cursor1.fetchall()

     ##### read the distinc cursor and submit SQS####
     for row in rowcnt: 
      
       print(row[0],row[1],row[2],row[3],row[4])

      ############################
      # try block for SQS message
      ############################
       try:
           response = queue.send_message( QueueUrl=queueUrl , 
              #MessageBody='"BookingID":{}, "Modificationdate":{}, "Bookingdate":{:%d %b %Y},"json_val":{}'.format(row[0],row[1],row[2],row[4]), 
              MessageBody='{}'.format(row[4]), 
              MessageAttributes={
                 'BookingAttr' : { 'StringValue': str(row[1]) , 'DataType': 'String' },
                 'SynchBatchRun': {'StringValue': now.strftime("%Y-%m-%d %H:%M"),'DataType' : 'String'}
                   } 
              ,MessageGroupId='bookings') 

           print("Response: {}".format(response.get('MessageId')))
      
           #cursor1.execute(del_query,(row[0], row[1], row[2],row[3]))
     
       except InvalidMessageContents as e:
           print("Missing parameter or send queue message call")
     #commit; 
     cursor1.close()
     #cnx.commit()
     #cnx.close()

    #######################################################
     ##### read the original cursor and start deleting ####
    #######################################################
     for row in rowcnt_orig: 
      
       print(row[0],row[1],row[2],row[3],row[4])

      ############################
      # try block for SQS message
      ############################
       try:
                 
           cursor_orig.execute(del_query,(row[0], row[1], row[2],row[3]))
     
       except InvalidMessageContents as e:
           print("Missing parameter or send queue message call")
     #commit; 
     cursor_orig.close()
     cnx.commit()
     cnx.close()

except mysql.connector.Error as err:
       if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your mysql user name or password")
       elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("mysql Database does not exist")
       else:
         print(err)
finally:
    print("function: Synch calendar ends.")
    cursor1.close()
    cursor_orig.close()
    cnx.close()
