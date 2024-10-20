import os
from dotenv import load_dotenv
import boto3
from utils.create_security_group import create_security_group
from utils.create_key_pair import generate_key_pair
from utils.run_command_instance import establish_ssh_connection,run_command
import time
import numpy as np
import matplotlib.pyplot as plt

#Retrieve the AWS credentials from the .env file
os.environ.pop('AWS_ACCESS_KEY_ID', None)
os.environ.pop('AWS_SECRET_ACCESS_KEY', None)
os.environ.pop('AWS_SESSION_TOKEN', None)
# Load .env file
load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')



key_pair_name = 'log8415E-tp2-key-pair'
# Create EC2 client
ec2 = boto3.client('ec2',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token = aws_session_token,
    region_name = "us-east-1"
)

key_pair_path = generate_key_pair(ec2, key_pair_name)
group_id = create_security_group(ec2, "log8415E-tp2", "none")

#Launch the EC2 instance

#Instance parameters
instance_params = {
        'ImageId': "ami-0e86e20dae9224db8", 
        'InstanceType': "t2.large",
        'MinCount': 1,
        'MaxCount': 1,
        'KeyName': key_pair_name,
        'SecurityGroupIds': [group_id],}

#Instance user data (Setting up Hadoop and Pyspark)
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
response = ec2.run_instances(UserData=user_data, **instance_params)
instance_id = response['Instances'][0]['InstanceId']
print(f"Instance {instance_id} is launching...")

ec2_resource = boto3.resource('ec2')
instance = ec2_resource.Instance(instance_id)

# instance.wait_until_running()  # Wait until the instance is running
# instance.reload() # Reload the instance data to get updated attributes
# public_ip = response['Instances'][0].get('PublicIpAddress') # Get the public IP address

public_ip = None
counter = 0
while public_ip is None:
    time.sleep(10)  # Wait for 10 seconds before checking the public IP address again
    counter += 10
    response = ec2.describe_instances(InstanceIds=[instance_id])
    print(counter, " seconds elapsed...")
    public_ip = response['Reservations'][0]['Instances'][0].get('PublicIpAddress')

print(f"The public IP address of instance {instance_id} is: {public_ip}")
time.sleep(100)


# Establish SSH connection with the EC2 instance
key_pair_path = str(key_pair_path)
ssh_connection = establish_ssh_connection(public_ip, key_pair_path)
print(ssh_connection)
print("SSH connection established with the EC2 instance...")
## Verify that the user data script has completed 
command = 'test -f /tmp/user_data_complete && echo "complete" || echo "incomplete"'

# Loop until the setup is complete
while True:
    output, error = run_command(ssh_connection, command)
    if output == "complete":
        print("Set up completed! Proceeding with Word Count...")
        break
    else:
        print("Waiting for Hadoop and Spark to complete...")
        time.sleep(120)  # Wait for 60 seconds before checking again

iterations = 3
datasets = ['https://www.gutenberg.ca/ebooks/buchanj-midwinter/buchanj-midwinter-00-t.txt',
            'https://www.gutenberg.ca/ebooks/carman-farhorizons/carman-farhorizons-00-t.txt',
            'https://www.gutenberg.ca/ebooks/colby-champlain/colby-champlain-00-t.txt',
            'https://www.gutenberg.ca/ebooks/cheyneyp-darkbahama/cheyneyp-darkbahama-00-t.txt',
            'https://www.gutenberg.ca/ebooks/delamare-bumps/delamare-bumps-00-t.txt',
            'https://www.gutenberg.ca/ebooks/charlesworth-scene/charlesworth-scene-00-t.txt',
            'https://www.gutenberg.ca/ebooks/delamare-lucy/delamare-lucy-00-t.txt',
            'https://www.gutenberg.ca/ebooks/delamare-myfanwy/delamare-myfanwy-00-t.txt',
            'https://www.gutenberg.ca/ebooks/delamare-penny/delamare-penny-00-t.txt'
            ]
hadoop_times = []
linux_times = []
spark_times = []

for dataset in datasets:
    
    file_name = dataset.split('/')[-1]
    
    hadoop_exec_times = []
    linux_exec_times = []
    spark_exec_times = []
    
    for _ in range(iterations):
        # Download dataset
        download_command = f"wget {dataset} -O {file_name}"
        output, error = run_command(ssh_connection, download_command)

        # Hadoop WordCount command
        wordCount_Hadoop_command = f"""
        /usr/local/hadoop/bin/hadoop fs -mkdir -p /input
        /usr/local/hadoop/bin/hadoop fs -put {file_name} /input
        /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar wordcount /input/{file_name} /output
        """
        # Measure Hadoop execution time
        start_time = time.time()
        output, error = run_command(ssh_connection, wordCount_Hadoop_command)
        hadoop_exec_times.append(time.time() - start_time)

        # Linux WordCount command
        wordCount_Linux_command = f"cat {file_name} | tr ' ' '\\n' | sort | uniq -c"
        # Measure Linux execution time
        start_time = time.time()
        output, error = run_command(ssh_connection, wordCount_Linux_command)
        linux_exec_times.append(time.time() - start_time)

        # Spark WordCount command
        START_COMMAND = f"/usr/local/spark/bin/spark-submit wordCount_spark.py /home/ubuntu/{file_name}"
        main_script = open("utils/wordCount_spark.py", "r").read()
        wordCount_spark_command = f"echo '{main_script}' > wordCount_spark.py && {START_COMMAND}"
        # Measure Spark execution time
        start_time = time.time()
        output, error = run_command(ssh_connection, wordCount_spark_command)
        spark_exec_times.append(time.time() - start_time)

    # Calculate the average execution times across iterations
    hadoop_times.append(np.mean(hadoop_exec_times))
    linux_times.append(np.mean(linux_exec_times))
    spark_times.append(np.mean(spark_exec_times))

# Plot the results
datasets_idx = np.linspace(1, len(datasets), len(datasets))
print("Performance comparison between Hadoop, Linux, and Spark:")
plt.figure(figsize=(12, 6))

# Hadoop vs. Linux comparison
plt.subplot(1, 2, 1)
plt.plot(datasets_idx, hadoop_times, label='Hadoop Execution Time', marker='o')
plt.plot(datasets_idx, linux_times, label='Linux Execution Time', marker='o')
plt.xlabel('Dataset')
plt.ylabel('Execution Time (seconds)')
plt.title('Hadoop vs Linux Performance Comparison')
plt.legend()
plt.grid(True)

# Hadoop vs. Spark comparison
plt.subplot(1, 2, 2)
plt.plot(datasets_idx, hadoop_times, label='Hadoop Execution Time', marker='o')
plt.plot(datasets_idx, spark_times, label='Spark Execution Time', marker='o')
plt.xlabel('Dataset')
plt.ylabel('Execution Time (seconds)')
plt.title('Hadoop vs Spark Performance Comparison')
plt.legend()
plt.grid(True)

plt.tight_layout()
plt.show()
    
# #Running the word count on the instance using Hadoop
# wordCount_Hadoop_command = """
# wget https://www.gutenberg.org/cache/epub/4300/pg4300.txt -O pg4300.txt
# /usr/local/hadoop/bin/hadoop fs -mkdir /input
# /usr/local/hadoop/bin/hadoop fs -put pg4300.txt /input
# /usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar wordcount /input/pg4300.txt /output
# /usr/local/hadoop/bin/hadoop fs -cat /output/part-r-00000 > output.txt
# cat output.txt
# """

# Hadoop_output, _ = run_command_on_ec2(public_ip,key_pair_path, wordCount_Hadoop_command)
# print('Hadoop output:','\n',Hadoop_output)


# #Running the word count on the instance using Linux
# wordCount_Linux_command = """ cat pg4300 . txt | tr ’ ’ ’\n’ | sort | uniq -c """
# Linux_output, _ = run_command_on_ec2(public_ip,key_pair_path, wordCount_Linux_command)
# print('Linux output:','\n',Linux_output)


# #Running the word count on the instance using Spark
# START_COMMAND = "/usr/local/spark/bin/spark-submit wordCount_spark.py /home/ubuntu/pg4300.txt"
# main_script = open("utils/wordCount_spark.py", "r").read()
# wordCount_spark_command = "echo '{}' > wordCount_spark.py && {}".format(main_script, START_COMMAND)
# Spark_output, _ = run_command_on_ec2(public_ip,key_pair_path, wordCount_spark_command)
# print('Spark output:','\n',Spark_output)


ssh_connection.close()