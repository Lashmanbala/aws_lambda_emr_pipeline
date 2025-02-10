# Github User Activity Pipeline - Aws Lambda, EMR

## Overview

The user activity in github is being recorded and stored in an archive called GH Archive. The archive is being updated every hour with last 1 hour user activity data as csv file. 
 
The requirement is to capture the data every hour  in s3 and process and store it in s3 in an optimzed way.

I used aws lambda function to download the data to s3 and the lambda function is triggerd by AWS Eventbridge every hour. 

And the data is processed and partitoned by spark in emr and stored as Parquet file in s3. The EMR cluster is lanched by a lambda function which is triggerd by eventbridge once a day in the midnight.



