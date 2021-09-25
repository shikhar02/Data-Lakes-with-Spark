SUMMARY
===============

  A startup company "Sparkify" want to move their data warehouse to data lake because of the increase in the user base as well as songs database even more. Currently, their data resides in S3. As a data engineer, my job is to build an ETL pipeline to extract data from S3, process them using Sparks and load the data back to S3 as a set of dimensional tabels.
  The main purpose of building a data lake is to help the "Sparkify" analytics team to continue finding insights related to their users likes and dislikes for songs.


RUNNING PYTHON SCRIPT
========================

Steps went into running 'etl.py' python script have been discussed below:

STEP-1 Create an AWS EMR cluster.

- I created EMR cluster using AWS CLI. The command I used was:
     
        aws emr create-cluster \
        name project_cluster \
        use-default-roles \
        release-label emr-5.28.0 \
        instance-count 3 \
        applications Name=Spark  \
        ec2-attributes KeyName=<key_name>,SubnetId=<subnet_id> \
        instance-type m5.2xlarge \
        profile sparks

Here, I used an instance type ___m5.xlarge___ with three instances.

STEP-2  Change Security group.

- By changing the security group of the master EC2 instance by adding an inbound rule for SSH protocol, we will be able to connect to master EC2 instance on an EMR cluster securely by making master EC2 instance to accept SSH protocol from our local computer.


STEP-3 Set Up an SSH Tunnel to the Master Node Using Dynamic Port Forwarding.

STEP-4 Copy EC2 log private key (.pem file) and python script (etl.py) to the master node securely from our local computer.

- Command -                   ___scp -i AWS_EC2_Demo.pem AWS_EC2_Demo.pem hadoop@ec2-3-139-93-181.us-east-2.compute.amazonaws.com:/home/hadoop/___


STEP-5 Connect to the EMR cluster.

- Command :                   ___ssh -i key_name.pem hadoop@ec1-9-140-123-167.us-east-2.compute.amazonaws.com ___


STEP-6 Run python script on EMR cluster.

- Command :                   ___usr/bin/spark-submit --master yarn ./<python_file_name>___


FILES IN THE REPOSITORY
===========================

- ___Songs Table data___: Songs table data file consist of __song_id, title, artist_id, year, duration__. This file is stored in S3 bucket (datalake2) in parquet format in the songs/songs.parquet directory. It is partitioned by ___year___ & ___artist_id___ columns. Parquet format is use to store data in more efficient way as it follows a columnar storage format in which data is stored column wise not as (traditional methods of adding data) row wise. Columnar Storage format improves the performance especially when working with big data.

- ___Artists Table data___: Artists table data file consist of __artist_id, artist_name, artist_location, artist_latitude, artist_longitude__. This file is stored (parquet format) under a directory named artists/artists.parquet. 

- ___Users Table data___: Users Table data file consist of __userId, firstName, lastName, gender, level__. This file is stored (parquet format) under a directory named users/users.parquet. 

- ___Time Table data___: Time table data file consist of __start_time, hour, day, week, month, year, weekday__. This file is stored (parquet format) under a directory named time/time.parquet and partitioned by ___year___ & ___month___.

- ___Songplays Table data___: Songplays table data file consist of __songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month__. This file is stored (parquet format) under a directory named songplays/songplays.parquet and partitioned by ___year___ & ___month___.


                                                        THANK YOU!
                                                        







