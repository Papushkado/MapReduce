from pyspark import SparkContext
from pathlib import Path


def process_friend_recommendation(file_path: Path, output_file: Path):
    """
    Process the friend recommendation data. Will create a new file with the recommendations. Each user defined in the input file
    will have a list of the top10 possible friends that shared the most common friends with the user. This function use Spark.
    :param file_path: Path to the input file. This is a .txt file. The format is the following: 
    <user_id><TAB><friend_id_a>,<friend_id_a>...
    """
    spark_context = SparkContext(appName="FriendRecommendation")
    # Load the data
    df = spark_context.read.text(str(file_path))
    # Split the data into columns   
    df = df.select(df.value.split('\t'))
    # Split the columns into separate columns

