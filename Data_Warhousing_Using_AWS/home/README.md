SPARKIFY ON AWS..

Sparkify will create the same practices as the one in local machine, except this will be hosted on AWS which acts as a data warehouse.

Building this project requires a good understanding of python wrapper for aws, which is Boto3, how to create a cluster on aws, difference between EC2, S3,and Redshift, saving your credentials into config file, and copying command from s3 to redshift. 

The critical steps that you must aknoweldge before starting the development are as follow:
1. Creating AWS Cluster:
There are two ways you can create an AWS cluster using AWS GUI, or using create_redshift_cluster.ipynb file. 
Before creating this cluster, you must edit the dwh.cfg file to include your aws credentials

2. Creating, insering, and transfering the tables:
the whole logic is layed in sql_queries.py
The logic will start by reading your 'cwh.cpg' to use them once we copy the staging table from s3 to redshift. Creating the table will create each table if it doesn't exist. Once all the tables get created, we will transfer the data from s3 to the staging table using the IAM ROLE and then insert the data, and this will be all done by running create_table.py. 

3. Developing the pipeline
This will be done by running ETL.PY
Will connect redshift using 'dwh.cfg' creds, execute load staging tables function, and insert table function.

After rnuuning it you should see something like this to know it ran successfully. 

"""
Connecting to redshift
Loading staging tables
Transform from staging
ETL Ended
"""

