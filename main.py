import os
from dotenv import load_dotenv
import boto3
from utils.clean_up import clean_up_instances
from utils.user_data import getUserData
from utils.create_security_group import create_security_group
from utils.create_key_pair import generate_key_pair
from utils.run_command_instance import establish_ssh_connection,run_command
import time
import numpy as np
import matplotlib.pyplot as plt
from scp import SCPClient


INSTANCES_INSTALL_DELAY = 800

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
        'SecurityGroupIds': [group_id],
        'BlockDeviceMappings':
        [
            {
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'VolumeSize': 16,
                    'VolumeType': 'standard'
                }
            }
        ]
}

response = ec2.run_instances(UserData=getUserData(), **instance_params)
instance_id = response['Instances'][0]['InstanceId']
print(f"Instance {instance_id} is launching...")

ec2_resource = boto3.resource('ec2')
instance = ec2_resource.Instance(instance_id)

# instance.wait_until_running()  # Wait until the instance is running
# instance.reload() # Reload the instance data to get updated attributes
# public_ip = instance.public_ip_address # Get the public IP address

public_ip = None
counter = 0
while public_ip is None:
    time.sleep(10)  # Wait for 10 seconds before checking the public IP address again
    counter += 10
    response = ec2.describe_instances(InstanceIds=[instance_id])
    print(counter, " seconds elapsed...")
    public_ip = response['Reservations'][0]['Instances'][0].get('PublicIpAddress')

print(f"The public IP address of instance {instance_id} is: {public_ip}")

print("Waiting for the instance to be ready...")
time.sleep(100)


# Establish SSH connection with the EC2 instance
key_pair_path = str(key_pair_path)
ssh_connection = establish_ssh_connection(public_ip, key_pair_path)
print(ssh_connection)
print("SSH connection established with the EC2 instance...")
## Verify that the user data script has completed 
command = 'test -f /tmp/user_data_complete && echo "complete" || echo "incomplete"'

# Loop until the setup is complete
counter = 0
while True:
    output, error = run_command(ssh_connection, command)
    if output == "complete":
        print("Set up completed! Proceeding with Word Count...")
        break
    else:
        print("Waiting for Hadoop and Spark to complete...")
        time.sleep(120)  # Wait for 60 seconds before checking again
        counter += 120
        print(counter, " seconds elapsed...")

iterations = 3
datasets = [
    'https://www.gutenberg.ca/ebooks/buchanj-midwinter/buchanj-midwinter-00-t.txt',
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

for idx, dataset in enumerate(datasets, start=1):
    file_name = dataset.split('/')[-1]
    
    hadoop_exec_times = []
    linux_exec_times = []
    spark_exec_times = []
    
    for iteration in range(iterations):
        print(f"Processing dataset {idx}/{len(datasets)}, iteration {iteration + 1}/{iterations}...")

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
        print(f"Hadoop word count executed for dataset {idx}/{len(datasets)}, iteration {iteration + 1}/{iterations}")

        # Linux WordCount command
        wordCount_Linux_command = f"cat {file_name} | tr ' ' '\\n' | sort | uniq -c"
        # Measure Linux execution time
        start_time = time.time()
        output, error = run_command(ssh_connection, wordCount_Linux_command)
        linux_exec_times.append(time.time() - start_time)
        print(f"Linux word count executed for dataset {idx}/{len(datasets)}, iteration {iteration + 1}/{iterations}")

        # Spark WordCount command
        START_COMMAND = f"/usr/local/spark/bin/spark-submit wordCount_spark.py /home/ubuntu/{file_name}"
        main_script = open("utils/wordCount_spark.py", "r").read()
        wordCount_spark_command = f"echo '{main_script}' > wordCount_spark.py && {START_COMMAND}"
        # Measure Spark execution time
        start_time = time.time()
        output, error = run_command(ssh_connection, wordCount_spark_command)
        spark_exec_times.append(time.time() - start_time)
        print(f"Spark word count executed for dataset {idx}/{len(datasets)}, iteration {iteration + 1}/{iterations}")

    # Calculate the average execution times across iterations
    hadoop_times.append(np.mean(hadoop_exec_times))
    linux_times.append(np.mean(linux_exec_times))
    spark_times.append(np.mean(spark_exec_times))

scp = SCPClient(ssh_connection.get_transport())
try:
    scp.put('friend_recommendation.py', '/home/ubuntu/friend_recommendation.py')
    print("Uploaded friend_recommendation.py successfully.")
    
    # Verify upload by checking if the file exists on EC2
    output, error = run_command(ssh_connection, 'ls /home/ubuntu/friend_recommendation.py')
    if output:
        print(f"Verification: friend_recommendation.py exists on EC2: {output}")

    scp.put('soc-LiveJournal1Adj.txt', '/home/ubuntu/soc-LiveJournal1Adj.txt')
    print("Uploaded soc-LiveJournal1Adj.txt successfully.")
    
    # Verify upload by checking if the file exists on EC2
    output, error = run_command(ssh_connection, 'ls /home/ubuntu/soc-LiveJournal1Adj.txt')
    if output:
        print(f"Verification: soc-LiveJournal1Adj.txt exists on EC2: {output}")
except Exception as e:
    print(f"File upload failed: {e}")
    exit(1)

# Run the friend recommendation Spark job
command = 'python3 /home/ubuntu/friend_recommendation.py'
print("Running friend recommendation...")
output, error = run_command(ssh_connection, command)
print(f"Friend Recommendation job output:\n{output}")
if error:
    print(f"Error during job execution:\n{error}")

# Fetch output (recommendations)
print("Fetching output from EC2 instance...")
try:
    scp.get('/home/ubuntu/output.txt', './output.txt')
    print("Output fetched and saved as output.txt.")
    
    # Verify the output file exists locally
    if os.path.exists('./output.txt'):
        print(f"Verification: output.txt successfully saved locally.")
except Exception as e:
    print(f"Failed to fetch output.txt: {e}")


scp.close()
print("SCP connection closed.")
ssh_connection.close()
print("SSH connection closed.")

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

##### In this part, we are going to clean_up, all the set up environnement

print("\n Cleaning up instances and security group ...")
clean_up_instances(ec2, [instance_id], key_pair_name, group_id)

