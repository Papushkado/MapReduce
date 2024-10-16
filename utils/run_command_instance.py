import paramiko
import time
import socket  # handle socket-related exceptions


def run_command_on_ec2(public_ip, key_pair_path, command, retries=5):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    for attempt in range(retries):
        try:
            print(f"Attempt {attempt + 1} to connect to {public_ip}")
            # Connect to the instance with increased timeout
            ssh.connect(hostname=public_ip, username='ubuntu', key_filename=key_pair_path, timeout=90)

            # Execute the command
            stdin, stdout, stderr = ssh.exec_command(command)
            
            # Get the output

            output = stdout.read().decode('utf-8').strip()
            error = stderr.read().decode('utf-8').strip()

            print(f"Command output: {output}")
            print(f"Command error: {error}")
            ssh.close()
            return output, error
        except paramiko.ssh_exception.SSHException as e:
            print(f"SSHException occurred: {e}")
            time.sleep(30)  # Wait before retrying
        except socket.timeout as e:
            print(f"Socket timeout occurred: {e}")
            time.sleep(30)  # Wait before retrying
        finally:
            ssh.close()

    return None, "Max retries reached. Unable to connect."

