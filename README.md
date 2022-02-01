# Project - Cloud Data Lake

## Abstract

In this project, I'll apply on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. 

To complete the project, I'll need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. 

I'm going to deploy this Spark process on a cluster using AWS.

## Project Steps

### Create AWS Account, EMR_EC2_DefaultRole and EMR_DefaultRole
- Set the permissions to the new user by attaching the AWS Managed AdministratorAccess policy from the list of existing policies.

```iam::32xxxx1183xx```


- Create default roles in IAM 
```shell
aws emr create-default-roles --profile <profile-name>
```

- Create EMR Cluster
  - Prerequisite
    - AWS CLI
      1. Download the file using the curl command.
      ```shell
      curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
      ```
      2. Run the standard macOS installer program, specifying the downloaded .pkg file as the source.
      ```shell
      sudo installer -pkg AWSCLIV2.pkg -target /
      ```
      3. To verify that the shell can find and run the aws command in my $PATH, use the following commands.
      ```shell
      which aws
      aws --version
      ```
    - Set up Access credentials using AWS IAM
      ```text
      Generate and save a new Access key (access key ID, and a secret key) locally in my system, which will allow my CLI to create an EMR cluster.
      ```
    - bootstrap
    ```shell
    #!/bin/bash
    # Downgrade gcc to get pandas to work and for murmurhash which is used by spacy
    #sudo yum remove -y gcc72 gcc gcc-c++
    sudo yum install -y gcc gcc-c++ tar bzip2
    sudo yum install -y python36 python36-devel python36-pip python36-setuptools python36-virtualenv
    sudo ln -s /usr/bin/pip-3.6 /usr/sbin/pip3

    sudo yum install -y tmux
    sudo yum install -y git
    sudo yum install -y vim
    sudo yum install -y blas lapack

    # change the bucket name
    aws s3 cp s3://mywsbucketbigdata/bootstrap_emr.sh $HOME/bootstraps --recursive

    # Set spark home (so that findspark finds spark)
    echo '
    export SPARK_HOME=/usr/lib/spark
    export PS1="\[\033[36m\]\u\[\033[m\]@\[\033[32m\]\h \[\033[33;1m\]\w\[\033[m\]\$ "
    export CLICOLOR=1
    export LSCOLORS=ExFxBxDxCxegedabagacad
    ' >> $HOME/.profile
    ```
    - EC2 Login Key-Pair
  - Create EMR Cluster
    ```shell
    aws emr create-cluster --name my_emr_cluster \
    --use-default-roles --release-label emr-5.28.0  \
    --instance-count 3 --applications Name=Spark Name=Hive Name=Zeppelin  \
    --bootstrap-actions Path="s3://mywsbucketbigdata/bootstrap_emr.sh" \
    --ec2-attributes KeyName=my-key-pair,SubnetId=subnet-ec57ca94 \
    --instance-type m5.xlarge \
    --log-uri s3://mywsbucketbigdata/emrlogs/ \
    --profile default
    #--auto-terminate \
    ```

### Build ETL Pipeline

- Execute etl.py
  - Source data
    ```
       Song dataset
       Log dataset
    ```
  - List All Tables
    ```
       songs_table
       artists_table
       user_table
       time_table
       songplays_table
    ```
  - Input data
  ```
  s3a://udacity-dend/
  ```
  - Output data
  ```
  s3://mywsbucketbigdata/project_tables
    artists_table/
    songplays_table/
    songs_table/
    time_table/
    user_table/
  ```