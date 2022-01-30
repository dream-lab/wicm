import pickle
import sys
from pyspark.sql import SparkSession

# get data on when edges are added and deleted
def getUnscaledTimepointEdgeStat(x):
    l = x.split(" ")
    insert_array = dict()
    delete_array = dict()
    for t in range(int(l[1]), int(l[2]) + 1):
        insert_array[t] = 0
        delete_array[t] = 0

    for eIndex in range(3, len(l), 3):
        start = int(l[eIndex + 1])
        end = int(l[eIndex + 2])

        insert_array[start] += 1
        delete_array[end] += 1

    result = [(('F', k), insert_array[k]) for k in insert_array.keys()]
    result.extend([(('R', k), delete_array[k]) for k in delete_array.keys()])
    return result


def getVertexList(x):
    l = x.split(" ")
    return int(l[0]), (int(l[1]), int(l[2]))

# fing timepoint edge distribution locally at vertex
def getVertexCumulativeTimepointEdges(x):
    l = x.split(" ")
    vStart = int(l[1])
    vEnd = int(l[2])
    vLife = vEnd - vStart

    insert_array = [0] * (vLife + 1)
    delete_array = [0] * (vLife + 1)
    for eIndex in range(3, len(l), 3):
        eStart = int(l[eIndex + 1])
        eEnd = int(l[eIndex + 2])
        insert_array[eStart - vStart] += 1
        delete_array[eEnd - vStart] += 1

    timepoint_edges = [0] * vLife
    cum_sum = 0
    for i in range(vLife):
        cum_sum += insert_array[i]
        cum_sum -= delete_array[i]
        timepoint_edges[i] = cum_sum

    forward_sum = [0] * vLife
    forward_sum[vLife - 1] = timepoint_edges[vLife - 1]
    for i in range(1, vLife):
        forward_sum[vLife - 1 - i] = timepoint_edges[vLife - 1 - i] + forward_sum[vLife - i]

    for i in range(vLife):
        forward_sum[i] += 1

    return int(l[0]), (vStart, forward_sum)


def getEdgeList(x):
    l = x.split(" ")
    edges = []
    for eIndex in range(3, len(l), 3):
        dest = int(l[eIndex])
        start = int(l[eIndex + 1])
        end = int(l[eIndex + 2])
        edges.append((dest, (start, end)))

    return edges

# for each interval edge, find overlapping timepoint edges
# scale corresponding timepoint edges of the interval edge
def getScaledTimepointEdgeStat(x):
    localArray = dict()

    vStart = x[1][1][0]
    forward_sum = x[1][1][1]

    for t in range(vStart, vStart + len(forward_sum)):
        localArray[t] = 0

    forward_sum.append(1)
    for eIndex in x[1][0]:
        eStart = eIndex[0]
        eEnd = eIndex[1]
        l = eEnd - eStart

        T_forward = sum(forward_sum[(eStart - vStart + 1):(eEnd - vStart + 1)])
        for step in range(eStart, eEnd):
            localArray[step] += (forward_sum[step - vStart + 1] * 1. * l) / T_forward

    result = [(('F', k), localArray[k]) for k in localArray.keys()]
    return result


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("GraphTimepoint") \
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    timepoint_edges_unscaled = lines.flatMap(lambda l: getUnscaledTimepointEdgeStat(l)).reduceByKey(lambda a, b: a + b)

    vList_cumulative = lines.map(lambda x: getVertexCumulativeTimepointEdges(x))
    eList = lines.flatMap(lambda x: getEdgeList(x)).groupByKey()
    veList_cumulative = eList.join(vList_cumulative)
    timepoint_edges_scaled = veList_cumulative.flatMap(lambda l: getScaledTimepointEdgeStat(l)).reduceByKey(
        lambda a, b: a + b)

    timepoint_edge_unscaled_dict = timepoint_edges_unscaled.collect()
    timepoint_edge_scaled_dict = timepoint_edges_scaled.collect()

    end = int(sys.argv[2])
    insert_edges = [0] * (end + 1)
    delete_edges = [0] * (end + 1)
    unscaled_timepoint_edges = [0] * end
    scaled_timepoint_edges = [0] * end

    for item in timepoint_edge_unscaled_dict:
        if item[0][0] == 'F':
            insert_edges[item[0][1]] = item[1]
        elif item[0][0] == 'R':
            delete_edges[item[0][1]] = item[1]

    for item in timepoint_edge_scaled_dict:
        if item[0][0] == 'F':
            scaled_timepoint_edges[item[0][1]] = item[1]

    spark.stop()

    cum_sum = 0
    for index in range(0, (len(insert_edges)-1)):
        cum_sum += insert_edges[index]
        cum_sum -= delete_edges[index]
        unscaled_timepoint_edges[index] = cum_sum

    f = open(sys.argv[3], 'wb')
    pickle.dump(insert_edges, f)
    pickle.dump(delete_edges, f)
    pickle.dump(unscaled_timepoint_edges, f)
    pickle.dump(scaled_timepoint_edges, f)
    f.close()
