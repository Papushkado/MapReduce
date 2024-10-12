from pyspark import SparkConf, SparkContext
from pathlib import Path
import sys


def map_line_into_user_and_friends(line):
    split = line.split("\t")
    #print(split)
    user_id = int(split[0])

    #if no ids after user_id, friends_ids is an empty str, else we split the line based on ,
    friends_ids = [] if split[1] == "" else list(map(lambda x: int(x), split[1].split(",") ))

    return user_id, friends_ids


def map_friends_connection(user_friends_entry):

    connections = []

    user = user_friends_entry[0]
    friends = user_friends_entry[1]

    for i in range(0, len(friends)):
        for j in range(i + 1, len(friends)): #avoid dupicates ex: ((2, 3), 1) and ((3, 2), 1). We assume that the friends are ordered
            connections.append((
                (min(friends[i], friends[j]), max(friends[j], friends[i])), 1)) #value of 1 is second hand friends
        connections.append(
            (
                (min(user, friends[i]), max(user, friends[i])), 0) #put lower id in left so we dont have duplicates
            ) # value of 0 is direct friend

    return connections

def map_connection_to_owner_recfriend_strength(line):
    rec_pair = line[0]
    strength = int(line[1])

    return [(rec_pair[0], (rec_pair[1], strength)), (rec_pair[1], (rec_pair[0], strength)) ]

def map_and_sort_friend_rec(line):
    user_id = line[0]
    ordered_friend_rec = list(line[1])
    ordered_friend_rec.sort(key = lambda x: x[1], reverse = True)
    ordered_friend_rec = [x[0] for x in ordered_friend_rec][:10] #take top 10 rec id

    return (user_id, ordered_friend_rec)


def process_friend_recommendation(file_path: Path, output_file: Path):
    """
    Process the friend recommendation data. Will create a new file with the recommendations. Each user defined in the input file
    will have a list of the top10 possible friends that shared the most common friends with the user. This function use Spark.
    :param file_path: Path to the input file. This is a .txt file. The format is the following: 
    <user_id><TAB><friend_id_a>,<friend_id_a>...
    """
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    # Load the data and split into user and friends
    user_friends_lines = sc.textFile(str(file_path)).map(map_line_into_user_and_friends)
    friends_connection = user_friends_lines.flatMap(map_friends_connection).groupByKey() # group every relation into 1 line

    #remove line if one of the connections is a 0. This mean they are a direct friend
    # for example ((usera, userb), [1,1,1,1,1,0,1,1,1,1]) will be removed because there exist a direct connection between (usera, userb)
    friends_filtered = friends_connection.filter(lambda line: 0 not in line[1]) 

    #we sum every list for every pair ((usera, userb), [1,1,1,1,1,1,1,1,1,1]) so that we get the overall strength
    # current format will be (user_id1, (rec_1, str1)), (user_id2, (rec_2, str2)), ...
    second_hand_connection_strength = friends_filtered.map(lambda line: (line[0], sum(line[1])))

    # we group by user_id key to group all recommendation in the same line
    #current format will be (user_id, [(rec_1, str1), (rec_3, str1), (rec_3, str1), (rec_3, str1)]), ....
    owner_rec_w_strength = second_hand_connection_strength.flatMap(map_connection_to_owner_recfriend_strength).groupByKey()

    # order and map the line so that we get : (user_id, [rec3, rec 4, rec1... ]) where rec were ordered based on the strength and we
    # took the top 10
    top10_friend_rec = owner_rec_w_strength.flatMap(map_and_sort_friend_rec)#.collect()

    top10_friend_rec.saveAsTextFile(str(output_file))

    sc.stop()


if __name__ == "__main__":
    file_path = Path("soc-LiveJournal1Adj.txt")
    out_path = Path("output.txt")

    process_friend_recommendation(file_path, out_path)