# Optimizing the Interval-centric Distributed Computing Model for Temporal Graph Algorithms
### Animesh Baranawal and Yogesh Simmhan
### To Appear in ACM EuroSys 2022

Built on Apache Giraph 1.3.0 and Hadoop 3.1.1 with support for YARN
(Linux Ubuntu-based system with >= 8GB RAM)

## Installing Graphite

Requirements:
 * Java JDK 8
 * Maven
 * Hadoop 3.1.1 (pseudo-distributed mode)

Instructions on setting up Hadoop 3.1.1 are present in `HadoopSetup.md`.
A jar with ICM code is present under jars. To install ICM:
```
cd jars
bash ./install.sh
```

## Building WICM

WICM source code is present under `src/`. To build the project, run the make script in `build/`.
```
cd build
bash ./make.sh
```

### Running an ICM job

With Graphite and Hadoop deployed, you can run your first temporal graph processing job. We will use the `EAT` example job which reads an input file of an interval graph in one of the supported formats and computes the breadth first traversal from a provided source node. We will use `IntIntNullTextInputFormat` input format. 

A sample graph `sampleGraph.txt` has been provided in `build/graphs` with ~30000 nodes ~1000000 edges. Each line above has the format `source_id startTime endTime dest_id startTime endTime ...`. To run `EAT`, giraph job script `runEAT.sh` has been provided in `build/scripts/giraph/icm`. The job script takes 4 arguments:

1. source : source from which the traversal algorithm will start
2. perfFlag : set to True to dump performance related log information
3. inputGraph : hdfs path to input graph
4. outputDir : hdfs path to output 

To run the script, first copy the sample graph file to HDFS:
```
hdfs dfs -copyFromLocal build/graphs/sampleGraph.txt
hdfs dfs -ls sampleGraph.txt # check if input copied
```

Running ICM mode job with sourceID as 0:
```
cd build
bash ./scripts/giraph/icm/runEAT.sh 0 false sampleGraph.txt output
```

### Running ICM with vertex-local optimisations

The related scripts are provided in `build/scripts/giraph/icm_luds`. The scripts have additional arguments compared to ICM scripts:

1. bufferSize : size of the message cache to be used in LU optimisation
2. minMsg : minimum message cardinality for LU optimisation

Running ICM mode job with sourceID as 0, buffersize of 100 and minMsg of 20:
```
cd build
bash ./scripts/giraph/icm_luds/runEAT.sh 0 100 20 false sampleGraph.txt output
```

### Running an WICM job 

The related scripts are provided in `build/scripts/giraph/wicm`. The scripts have additional arguments compared to ICM scripts:

1. lowerE : start time of the graph, typically 0
2. upperE : end time of the graph
3. windows : temporal partitioning of the graph

The sample graph has lifespan [0,40). Assume the split strategy to be [0,20), [20,30) and [30,40). Running WICM mode job with same source:
```
cd build
bash ./scripts/runEAT.sh 0 0 40 "0;20;30;40" false sampleGraph.txt output
```

Scripts for running WICM with vertex-local optimisations are present under `build/scripts/giraph/wicm_luds`.


### Minimal experimental pipeline

Job scripts have been provided for 6 traversal algorithms: EAT, SSSP, TR, LD, TMST and FAST. Notice that the job is computed using a single worker using the argument `-w` and multiple threads using the argument `numComputeThreads`. The job script can be modified to use more workers or more computation threads.

A minimal experiment script has been provided `build/scripts/giraph/runExperiments.sh`. It runs all algorithms across 2 different sources and 2 different temporal partitionings. At the end of each job, the script compares the job output with ICM output using `build/scripts/giraph/compare.sh`.
To run the pipeline:

```
cd build
bash ./scripts/giraph/runExperiments.sh
```

## Running Heuristic for obtaining splits

Requirements:
 * Hadoop 3.1.1
 * Apache Spark 3.1.2
 * Python >= 2.7

Instructions for setting up Apache Spark are present in `SparkSetup.md`.
Hadoop setup is required before running Spark.

Pyspark code has been provided for obtaining the timepoint edge distribution required for running the heuristic.
The code is present in `build/scripts/heuristic/getGraphTimepoint_spark.py`. It uses the same input graph format as described under `Running an ICM job`. The script takes 3 arguments:
1. input : hdfs path to input graph
2. upperE : end time of input graph (assumption that start time of graph is 0)
3. output : local path for storing obtained distributions

To run this pyspark code on the input graph:
```
cd build/scripts/heuristic
spark-submit --master yarn --num-executors 1 --executor-cores 1 --executor-memory 2G getGraphTimepoint_spark.py sampleGraph.txt 40 sampleGraph.bin
```

The obtained distribution is present as `sampleGraph.bin` in `build/graphs/distribution`. The heuristic code is available in `build/scripts/heuristic/split.py`. The code takes the edge distribution binary file as input and prints the split strategy obtained by running the heuristic on the distribution. 
```
cd build/scripts/heuristic
python split.py ../../graphs/distributions/sampleGraph.bin
```
## Evaluated Graphs

The evaluated graphs can be obtained with some preprocessing from the following links:
1. Reddit: https://www.cs.cornell.edu/~jhessel/projectPages/redditHRC.html
2. WebUK: http://law.di.unimi.it/webdata/uk-union-2006-06-2007-05/
3. MAG: https://www.microsoft.com/en-us/research/project/open-academic-graph/
4. Twitter: http://twitter.mpi-sws.org/
5. LDBC-8_9-FB: datagen-8_9-fb - https://graphalytics.org/datasets
6. LDBC-9_0-FB: datagen-9_0-fb - https://graphalytics.org/datasets

Edge Distribution files for all the evaluated graphs are present under `build/graphs/distributions`. 
