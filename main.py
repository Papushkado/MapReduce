import os
import time
from dotenv import load_dotenv
import boto3
from utils.create_security_group import create_security_group
from utils.create_key_pair import generate_key_pair
from utils.run_command_instance import run_command_on_ec2

# Charger les informations d'identification AWS à partir de .env (Comme pour le TP1)
load_dotenv(dotenv_path='./.env')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')

# Nom de la clé SSH et groupe de sécurité
key_pair_name = 'log8415E-tp2-key-pair'
security_group_name = 'log8415E-tp2-security-group'

# Créer le client EC2
ec2_client = boto3.client('ec2',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name="us-east-1"
)

# Créer la clé et le groupe de sécurité
key_pair_path = generate_key_pair(ec2_client, key_pair_name)
group_id = create_security_group(ec2_client, security_group_name, "none")

# Paramètres d'instance EC2
instance_params = {
    'ImageId': "ami-0e86e20dae9224db8",  # AMI Ubuntu
    'InstanceType': "t2.micro",
    'MinCount': 1,
    'MaxCount': 1,
    'KeyName': key_pair_name,
    'SecurityGroupIds': [group_id]
}

# Script de configuration initiale de Hadoop et Spark sur l'instance EC2
user_data = """#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y bash wget coreutils default-jdk
sudo wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
sudo tar -xzvf hadoop-3.4.0.tar.gz
sudo mv hadoop-3.4.0 /usr/local/hadoop
JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
echo "export JAVA_HOME=$JAVA_HOME" | sudo tee -a /usr/local/hadoop/etc/hadoop/hadoop-env.sh
sudo wget https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
sudo tar -xzvf spark-3.5.3-bin-hadoop3.tgz
sudo mv spark-3.5.3-bin-hadoop3 /usr/local/spark
sudo apt-get install python3 python3-pip -y
sudo pip3 install pyspark
sudo bash -c 'echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment'
sudo bash -c 'echo "HADOOP_HOME=/usr/local/hadoop" >> /etc/environment'
sudo bash -c 'echo "SPARK_HOME=/usr/local/spark" >> /etc/environment'
sudo bash -c 'echo "PATH=/usr/local/hadoop/bin:/usr/local/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" >> /etc/environment'
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:/usr/local/hadoop/bin:/usr/local/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
/usr/local/hadoop/bin/hadoop version
/usr/local/spark/bin/spark-submit --version
touch /tmp/user_data_complete
"""