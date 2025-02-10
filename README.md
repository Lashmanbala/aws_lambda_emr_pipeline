# Github User Activity Pipeline - AWS Lambda, EMR

## Overview

The user activity in github is being recorded and stored in an archive called GH Archive. The archive is being updated every hour with last 1 hour user activity data as csv file. 
 
The requirement is to capture the data every hour  in s3 and process and store it in s3 in an optimzed way.

I used aws lambda function to download the data to s3 and the lambda function is triggerd by AWS Eventbridge every hour. 

And the data is processed and partitoned by spark in emr and stored as Parquet file in s3. The EMR cluster is lanched by a lambda function which is triggerd by eventbridge once a day in the midnight.

I have built a custom python sctipt which will take care of creating s3 buckets, iam roles, lambda functions, emr cluster and configuring evet bridge rule to trigger the lambdas functions.

By running that python script everything will get deployed in AWS.

## Setup
To setup this project locally, follow these steps

1. **Clone This Repositories:**
     ```bash
     git clone https://github.com/Lashmanbala/aws_lambda_emr_pipeline
     ```

2. **Configure AWS**

   Configure your aws account with your credentials in your local machine.

4. **Create .env file**

   Create .env file in the aws_resources directory, by refering the sample.env file.
   
4. **Update the script**
   
   Update the file paths and  resource names with your values in the app.py script in aws_resources directory.

   Update the BASELINE_FILE variable in the create_downloder_lambda function from when the past files should be downloaded.

5. **Run the app**
     ```bash
     cd aws_resources
     python3 app.py
     ```

## Contact
For any questions, issues, or suggestions, please feel free to contact the project maintainer:

GitHub: [Lashmanbala](https://github.com/Lashmanbala)

LinkedIn: [Lashmanbala](https://www.linkedin.com/in/lashmanbala/)
