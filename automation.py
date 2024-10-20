import os
from dotenv import load_dotenv
import boto3
from utils.create_security_group import create_security_group
from utils.create_key_pair import generate_key_pair
from utils.run_command_instance import establish_ssh_connection, run_command
from scp import SCPClient
import time
from paramiko.ssh_exception import SSHException

# Load AWS credentials
os.environ.pop('AWS_ACCESS_KEY_ID', None)
os.environ.pop('AWS_SECRET_ACCESS_KEY', None)
os.environ.pop('AWS_SESSION_TOKEN', None)
load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')

# EC2 parameters
key_pair_name = 'log8415E-tp2-key-pair'
ec2 = boto3.client('ec2', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token, region_name="us-east-1")
key_pair_path = generate_key_pair(ec2, key_pair_name)
group_id = create_security_group(ec2, "log8415E-tp2", "none")

#Instance parameters
instance_params = {
        'ImageId': "ami-0e86e20dae9224db8", 
        'InstanceType': "t2.large",
        'MinCount': 1,
        'MaxCount': 1,
        'KeyName': key_pair_name,
        'SecurityGroupIds': [group_id],}

#Instance user data (Setting up Hadoop and Pyspark)

user_data = r"""#!/bin/bash
# Update and upgrade the system
# Update and upgrade the system
sudo apt-get update && sudo apt-get upgrade -y

# Ensure the universe repository is enabled
sudo add-apt-repository universe
sudo apt-get update

# Install necessary packages: Java, Python3, and pip3
sudo apt-get install -y default-jdk python3 python3-pip

# Install PySpark globally
sudo pip3 install pyspark --break-system-packages

# Also install PySpark for the current user (non-sudo installation)
pip3 install --user --upgrade pyspark


# Ensure permissions for PySpark (global and user-specific installations)
PYSPARK_PATH=$(pip show pyspark | grep Location | cut -d' ' -f2)
if [ -d "$PYSPARK_PATH" ]; then
    sudo chmod -R ugo+rX $PYSPARK_PATH
else
    echo "PySpark path not found: $PYSPARK_PATH"
fi

# Set environment variables for Java and Spark
sudo bash -c 'echo "JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")" >> /etc/environment'
sudo bash -c 'echo "SPARK_HOME=/usr/local/spark" >> /etc/environment'
sudo bash -c 'echo "PYTHONPATH=\$PYTHONPATH:/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9-src.zip" >> /etc/environment'
sudo bash -c 'echo "PATH=\$PATH:~/.local/bin:/usr/local/spark/bin" >> /etc/environment'

# Set local user environment variables for pip packages
echo "export PATH=\$PATH:~/.local/bin" >> ~/.bashrc
echo "export PYTHONPATH=\$PYTHONPATH:/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9-src.zip" >> ~/.bashrc
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc

# Apply the changes for the current session
source ~/.bashrc

# Verify installations for debugging purposes
/usr/local/spark/bin/spark-submit --version || echo "Spark installation failed"
python3 -m pip show pyspark || echo "PySpark installation failed"

# Test PySpark import as a verification step
python3 -c "import pyspark; print('PySpark imported successfully')" || echo "Failed to import PySpark"

# Create a marker file to signal that the setup is complete
touch /tmp/user_data_complete
"""


print("Launching EC2 instance...")
response = ec2.run_instances(UserData=user_data, **instance_params)
instance_id = response['Instances'][0]['InstanceId']
print(f"Instance {instance_id} launched successfully.")

# Wait for instance setup
ec2_resource = boto3.resource('ec2')
instance = ec2_resource.Instance(instance_id)

# Check for public IP address
public_ip = None
counter = 0
while public_ip is None:
    time.sleep(10)  # Wait for 10 seconds before checking the public IP address again
    counter += 10
    response = ec2.describe_instances(InstanceIds=[instance_id])
    print(f"{counter} seconds elapsed, waiting for the public IP address...")
    public_ip = response['Reservations'][0]['Instances'][0].get('PublicIpAddress')

print(f"Instance public IP address: {public_ip}")
# Establish SSH connection to the instance
time.sleep(150)  # Wait for 100 seconds before attempting to establish an SSH connection
print("Establishing SSH connection...")
key_pair_path = str(key_pair_path)
ssh_connection = establish_ssh_connection(public_ip, key_pair_path)
print(ssh_connection)
print("SSH connection established with the EC2 instance...")

# Upload files using SCP (from root directory)
print("Uploading files to EC2 instance...")
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


# Monitor job completion (check if setup is done)
print("Monitoring job completion...")
command = 'test -f /tmp/user_data_complete && echo "complete" || echo "incomplete"'

while True:
    output, error = run_command(ssh_connection, command)
    if output == "complete":
        print("Set up completed! Proceeding with friend recommendation...")
        break
    else:
        print("Waiting for installation to be completed...")
        time.sleep(120)  # Wait for 60 seconds before checking again



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

# Close SCP and SSH connections
scp.close()
ssh_connection.close()
print("SSH and SCP connections closed.")
