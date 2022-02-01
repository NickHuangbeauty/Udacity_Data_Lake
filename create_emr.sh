#!/bin/bash

# EMR Cluster Name:  my_emr_cluster
# instance-count 3: 1 master, 2 slaves
# pre-install application Spark and Zeppelin
# key pair file name: my-key-pair
# Be created instance type: m5.xlarge
# Save log to s3://mywsbucketbigdata/emrlogs/

set -x

aws emr create-cluster --name my_emr_cluster \
 --use-default-roles --release-label emr-5.28.0  \
--instance-count 3 --applications Name=Spark Name=Hive Name=Zeppelin  \
--bootstrap-actions Path="s3://mywsbucketbigdata/bootstrap_emr.sh" \
--ec2-attributes KeyName=my-key-pair,SubnetId=subnet-ec57ca94 \
--instance-type m5.xlarge \
--log-uri s3://mywsbucketbigdata/emrlogs/ \
--profile default
#--auto-terminate \
