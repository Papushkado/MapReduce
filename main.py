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

# Lancer l'instance EC2
response = ec2_client.run_instances(UserData=user_data, **instance_params)
instance_id = response['Instances'][0]['InstanceId']
print(f"Instance {instance_id} is launching...")

# Attendre que l'instance soit en cours d'exécution
ec2_resource = boto3.resource('ec2')
instance = ec2_resource.Instance(instance_id)
instance.wait_until_running()
instance.reload()  # Mettre à jour l'IP publique
public_ip = instance.public_ip_address
print(f"The public IP address of instance {instance_id} is: {public_ip}")
time.sleep(100)  # Attendre que l'instance soit prête

# Vérification de l'exécution du script de configuration
command = 'test -f /tmp/user_data_complete && echo "complete" || echo "incomplete"'
while True:
    # Convertir key_pair_path en chaîne pour l'utiliser avec paramiko
    output, _ = run_command_on_ec2(public_ip, str(key_pair_path), command)
    if output == "complete":
        print("Setup completed! Proceeding with Word Count and Friend Recommendation...")
        break
    else:
        print("Waiting for Hadoop and Spark setup to complete...")
        time.sleep(60)

        
# Exécution du WordCount avec Hadoop, Linux, et Spark
wordCount_Hadoop_command = """
/usr/local/hadoop/bin/hadoop fs -mkdir /input
/usr/local/hadoop/bin/hadoop fs -put pg4300.txt /input
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar wordcount /input/pg4300.txt /output
/usr/local/hadoop/bin/hadoop fs -cat /output/part-r-00000 > output_hadoop.txt
cat output_hadoop.txt
"""
hadoop_output, _ = run_command_on_ec2(public_ip, key_pair_path, wordCount_Hadoop_command)
print('Hadoop WordCount output:', hadoop_output)

wordCount_Linux_command = "cat pg4300.txt | tr ' ' '\\n' | sort | uniq -c > output_linux.txt"
linux_output, _ = run_command_on_ec2(public_ip, key_pair_path, wordCount_Linux_command)
print('Linux WordCount output:', linux_output)

spark_command = "/usr/local/spark/bin/spark-submit wordCount_spark.py /home/ubuntu/pg4300.txt /home/ubuntu/output_spark.txt"
run_command_on_ec2(public_ip, key_pair_path, spark_command)

# Exécuter la recommandation d'amis
friend_recommendation_command = "/usr/local/spark/bin/spark-submit friend_recommendation.py"
friend_output, _ = run_command_on_ec2(public_ip, key_pair_path, friend_recommendation_command)
print('Friend Recommendation output:', friend_output)

# Nettoyage : arrêter et terminer l'instance EC2 après exécution
ec2_client.terminate_instances(InstanceIds=[instance_id])
print(f"Instance {instance_id} terminée.")

time.sleep(300)
# Supprimer la Key Pair
ec2_client.delete_key_pair(KeyName=key_pair_name)
print(f"Key pair '{key_pair_name}' supprimée.")

# Supprimer le Security Group
ec2_client.delete_security_group(GroupId=group_id)
print(f"Security group '{security_group_name}' supprimé.")

