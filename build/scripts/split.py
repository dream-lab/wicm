import sys
import pickle

import math

def GetEdgeOverlapRatio(splitStrategy, insertEdges, deleteEdges):
    totalIntervalEdges = sum(insertEdges)

    windowIntervalEdges = 0
    for i in range(len(splitStrategy)-1):
        windowStart = splitStrategy[i]
        windowEnd = splitStrategy[i+1]
        windowEdges = sum(insertEdges[:windowEnd]) - sum(deleteEdges[:(windowStart+1)])
        windowIntervalEdges += windowEdges

    return windowIntervalEdges/totalIntervalEdges


def EquiDepthHistogramBinning(distribution, nBins):
    totalFrequency = sum(distribution)
    maxBinLabel = len(distribution)
    approxFrequencyPerBin = totalFrequency / nBins

    ## naive
    binBoundaries = [0]
    computedBins = 0
    binLabel = 0
    frequency = 0

    while (computedBins < nBins) and (binLabel < maxBinLabel):
        if int(math.floor(frequency)) > int(math.floor((computedBins + 1) * approxFrequencyPerBin)):
            binBoundaries.append(binLabel)
            computedBins += 1
            continue

        frequency += distribution[binLabel]
        binLabel += 1

    binBoundaries.append(binLabel)
    return binBoundaries


def FindWindows(insertEdges, deleteEdges, timepointEdges):
    beta = math.inf
    strategy = None
    maxPartitions = len(timepointEdges)
    seenPartitions = set()

    for numSplit in range(2, int(math.ceil(math.sqrt(maxPartitions)))+1):
        splitStrategy = EquiDepthHistogramBinning(timepointEdges, numSplit)
        numWindows = (len(splitStrategy)-1)
        if numWindows not in seenPartitions:
            seenPartitions.add(numWindows)
        else:
            continue

        alpha = GetEdgeOverlapRatio(splitStrategy, insertEdges, deleteEdges)
        new_beta = math.pow(alpha,2)/numSplit
        if new_beta < beta:
            beta = new_beta
            strategy = splitStrategy

    return beta, strategy


# MAIN
# 1. read edge distribution
f = open(sys.argv[1], 'rb')
insert_edges = pickle.load(f)
delete_edges = pickle.load(f)
timepoint_edges = pickle.load(f)
scaled_timepoint_edges = pickle.load(f)
f.close()

assert abs(sum(insert_edges) - sum(delete_edges)) < 0.01
assert abs(sum(timepoint_edges) - sum(scaled_timepoint_edges)) < 0.01

start = 0
end = len(insert_edges) - 1
size = end - start + 1

# 2. find split based on heuristic
print(FindWindows(insert_edges, delete_edges, timepoint_edges))
print(FindWindows(insert_edges, delete_edges, scaled_timepoint_edges))