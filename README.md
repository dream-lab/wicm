# WICM

Distributed Temporal Graph Processing System using Graphite + Window ICM

Built on Apache Giraph 1.3.0 and Hadoop 3.1.1 with support for YARN

## Installing Graphite

Requirements:
 * Java JDK 1.8
 * Maven
 * Hadoop 3.1.1

A jar with ICM code is present under jars. To install ICM:
```
cd jars
./install.sh
```

## Building WICM

WICM source code is present under `src/`. To build the project, run the make script in `build/`.
```
cd build
./make.sh
```

## Running a Graphite / WICM job

With Graphite and Hadoop deployed, you can run your first temporal graph processing job. We will use the `EAT` example job which reads an input file of an interval graph in one of the supported formats and computes the breadth first traversal from a provided source node. We will use `IntIntNullTextInputFormat` input format. 

A sample graph has been provided in `build/graphs` with ~3500 nodes ~10000 edges. Each line above has the format `source_id startTime endTime dest_id startTime endTime ...`. To run `EAT`, giraph job script `runEAT.sh` has been provided in `build/scripts`. The job script supports 2 modes

1. ICM : This is the default graphite mode. The script takes 4 arguments: sourceId, debugFlag, inputGraph, outputDirectory in respective order.
2. WICM : This is the window ICM mode. The script takes 7 arguments: sourceId, Graph start time, Graph end time, window splits, debugFlag, inputGraph, outputDirectory.

To run the script, first copy the sample graph file to HDFS:
```
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal build/graphs/sampleGraph.txt /tmp/input/small_graph.txt
$HADOOP_HOME/bin/hdfs dfs -ls /tmp/input
```

Running ICM mode job with sourceID as 0:
```
build/scripts/runEAT.sh ICM 0 true /tmp/input/small_graph.txt output
```

The small graph has lifespan [0,36). Assume the split strategy to be [0,10), [10,20) and [20,36). Running WICM mode job with same source:
```
build/scripts/runEAT.sh WICM 0 0 36 0;10;20;36 true /tmp/input/small_graph.txt output
```

The job script also has a `compare` mode which compares the output of ICM mode and WICM mode using `diff`. It prints ''Equivalent'' if the diff is empty.
```
build/scripts/runEAT.sh compare output_icm output_wicm
```

Job scripts have been provided for 6 traversal algorithms: EAT, SSSP, TR, LD, TMST and FAST. Notice that the job is computed using a single worker using the argument `-w` and multiple threads using the argument `numComputeThreads`. The job script can be modified to use more workers or more computation threads

## Running Heuristic for obtaining splits

The heuristic code is available in `build/scripts` with the name `split.py`. The code takes the edge distribution of the graph as input and prints the split strategy obtained by running the heuristic on the distribution. Distributions for all the evaluated graphs are present under `build/graphs/distributions`. 

The heuristic code has been tested on `Python3`. To run the heuristic code on distribution of WebUK graph:
```
python3 build/scripts/split.py build/graphs/distributions/WebUK_distribution.py
```
The output is a tuple  (&#946; , split strategy) where split strategy is an array. For example, if the split strategy is `[0,10,20,36]`, then the obtained splits are [0,10), [10,20) and [20,36). 

## Evaluated Graphs

The evaluated graphs can be obtained from the following links:
1. Reddit: https://www.cs.cornell.edu/~jhessel/projectPages/redditHRC.html
2. WebUK: http://law.di.unimi.it/webdata/uk-union-2006-06-2007-05/
3. MAG: https://www.microsoft.com/en-us/research/project/open-academic-graph/
4. Twitter: http://twitter.mpi-sws.org/
5. LDBC-8_9-FB: datagen-8_9-fb - https://graphalytics.org/datasets
6. LDBC-9_0-FB: datagen-9_0-fb - https://graphalytics.org/datasets