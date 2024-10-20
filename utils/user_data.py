def getUserData():
    return """#!/bin/bash
    # python3 -m venv myenv
    # source myenv/bin/activate

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
    sudo pip3 install pyspark --break-system-packages



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