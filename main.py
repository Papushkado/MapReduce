import os
import time
from dotenv import load_dotenv
import boto3
import matplotlib.pyplot as plt
from utils.create_security_group import create_security_group
from utils.create_key_pair import generate_key_pair
from utils.run_command_instance import run_command_on_ec2

# Charger les informations d'identification AWS à partir de .env
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
# ... (rest of your user_data script) ...
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
    output, _ = run_command_on_ec2(public_ip, str(key_pair_path), command)
    if output == "complete":
        print("Setup completed! Proceeding with Word Count and Friend Recommendation...")
        break
    else:
        print("Waiting for Hadoop and Spark setup to complete...")
        time.sleep(60)

# Télécharger le fichier pg4300.txt avant d'exécuter les commandes
download_pg4300_command = "wget https://www.gutenberg.org/cache/epub/4300/pg4300.txt -O pg4300.txt"
run_command_on_ec2(public_ip, str(key_pair_path), download_pg4300_command)

# Upload du script Python WordCount Spark
upload_wordcount_script_command = "echo '{}' > /home/ubuntu/wordCount_spark.py".format(open('/utils/wordCount_spark.py').read())
run_command_on_ec2(public_ip, str(key_pair_path), upload_wordcount_script_command)

# Upload du script Python Recommandation d'amis
upload_friend_recommendation_command = "echo '{}' > /home/ubuntu/friend_recommendation.py".format(open('friend_recommendation.py').read())
run_command_on_ec2(public_ip, str(key_pair_path), upload_friend_recommendation_command)

# Exécution du WordCount avec Hadoop
start_time_hadoop = time.time()
wordCount_Hadoop_command = """
/usr/local/hadoop/bin/hadoop fs -mkdir -p /input
/usr/local/hadoop/bin/hadoop fs -put pg4300.txt /input
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar wordcount /input/pg4300.txt /output
/usr/local/hadoop/bin/hadoop fs -cat /output/part-r-00000 > output_hadoop.txt
cat output_hadoop.txt
"""
hadoop_output, _ = run_command_on_ec2(public_ip, str(key_pair_path), wordCount_Hadoop_command)
execution_time_hadoop = time.time() - start_time_hadoop
print('Hadoop WordCount output:', hadoop_output)

# Exécution du WordCount avec Linux
start_time_linux = time.time()
wordCount_Linux_command = "cat pg4300.txt | tr ' ' '\\n' | sort | uniq -c > output_linux.txt"
linux_output, _ = run_command_on_ec2(public_ip, str(key_pair_path), wordCount_Linux_command)
execution_time_linux = time.time() - start_time_linux
print('Linux WordCount output:', linux_output)

# Exécution du WordCount avec Spark
start_time_spark = time.time()
spark_command = "/usr/local/spark/bin/spark-submit /home/ubuntu/wordCount_spark.py /home/ubuntu/pg4300.txt /home/ubuntu/output_spark.txt"
spark_output, _ = run_command_on_ec2(public_ip, str(key_pair_path), spark_command)
execution_time_spark = time.time() - start_time_spark

# Exécuter la recommandation d'amis
friend_recommendation_command = "/usr/local/spark/bin/spark-submit /home/ubuntu/friend_recommendation.py"
friend_output, _ = run_command_on_ec2(public_ip, str(key_pair_path), friend_recommendation_command)
print('Friend Recommendation output:', friend_output)

# Nettoyage : arrêter et terminer l'instance EC2 après exécution
ec2_client.terminate_instances(InstanceIds=[instance_id])
print(f"Instance {instance_id} terminée.")

# Attendre la terminaison de l'instance
instance.wait_until_terminated()

# Supprimer la Key Pair
ec2_client.delete_key_pair(KeyName=key_pair_name)
print(f"Key pair '{key_pair_name}' supprimée.")

# Supprimer le Security Group
ec2_client.delete_security_group(GroupId=group_id)
print(f"Security group '{security_group_name}' supprimé.")

# Graphiques
datasets = ['Hadoop', 'Linux', 'Spark']
execution_times = [execution_time_hadoop, execution_time_linux, execution_time_spark]

# Création des graphiques
plt.figure(figsize=(15, 5))

# Graphique 1 : Temps d'exécution de Hadoop vs Linux
plt.subplot(1, 3, 1)
plt.bar(datasets[:2], execution_times[:2], color=['blue', 'orange'])
plt.title('Hadoop vs Linux')
plt.ylabel('Execution Time (seconds)')

# Graphique 2 : Temps d'exécution de Hadoop vs Spark
plt.subplot(1, 3, 2)
plt.bar(datasets[::2], execution_times[::2], color=['blue', 'green'])
plt.title('Hadoop vs Spark')

# Graphique 3 : Temps d'exécution de tous les trois
plt.subplot(1, 3, 3)
plt.bar(datasets, execution_times, color=['blue', 'orange', 'green'])
plt.title('All Execution Times')

# Afficher les graphiques
plt.tight_layout()
plt.show()
