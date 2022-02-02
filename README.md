# Optimizing the Interval-centric Distributed Computing Model for Temporal Graph Algorithms
#### Animesh Baranawal and [Yogesh Simmhan](http://cds.iisc.ac.in/faculty/simmhan/)
#### To Appear in [*ACM EuroSys 2022*](https://2022.eurosys.org/)

Temporal graphs are ones where lifespans are present on vertices, edges and attributes. Large temporal graphs are common in logistics and transit networks, social and web graphs, and in COVID-19 contact graphs. The *Interval-centric Computing Model (ICM)* extends *Google's Pregel Vertex-centric Computing Model (VCM)* to intuitively compose and execute temporal graph algorithms in a distributed system. But the *TimeWarp* operation and *scatter messaging* in ICM impose performance penalties. 

In this paper, we propose a number of optimizations to ICM to mitigate these effects. We Locally Unroll (LU) messages and defer the execution of the message scatter phase (DS) within a superstep of ICM to reduce the time complexity of TimeWarp and the message replication overheads. We also temporally partition the interval graph into windows of subgraphs (WICM) to spread the load across more windows of execution and reduce the time-complexity of TimeWarp and messaging. These techniques do not affect the correctness of the graph algorithms, or their equivalence with ICM, if the user compute logic is commutative, associative,  distributive and a selection function.




## About this Repository
The  **Windowed Interval-centric Computing Model (WICM)** platform provided in this repository is the implementation of the EuroSys 2022 paper. WICM is built on top of Graphite (which implements ICM) [[ICDE 2020](https://doi.org/10.1109/ICDE48307.2020.00102)], [Apache Giraph 1.3.0](https://giraph.apache.org/releases.html) (which implements Pregel VCM), and [Hadoop 3.1.1](https://hadoop.apache.org/release/3.1.1.html) with support for HDFS and YARN. We provide instructions for installing and running WICM in a pseudo-distributed mode on a single machine. 

These instructions help install WICM and ICM, which are both compared in the paper. **The goal is ensure that the artifacts can be evaluated to be [Functional](https://sysartifacts.github.io/eurosys2022/badges)**, i.e., *the artifacts associated with the research are found to be documented, consistent, complete, exercisable, and include appropriate evidence of verification and validation.*

We first install ICM and WICM, and then run the Earliest Arrival Time (EAT) algorithm that is evaluated in the paper on these platform variants, specifically (1) ICM, (2) ICM+LU+DS optimizations, (3) WICM optimization, (4) WICM+LU+DS optimizations, and (5) Heuristics for WICM. #1 is the prior baseline while the rest are the contributions of the paper. We also provide scripts to run *all six graph algorithms* used in the paper on a sample graph, as well as to run the WICM heuristics for finding the window splits. Links to the *six large graphs* evaluated in the paper are provided. We also offer scripts to verify the correctness of the outputs. 


---
## 1. Installing Graphite ICM

Pre-requisites:
 * A Linux Ubuntu-based system (VM or bare-metal) with >= 8GB RAM
 * Java JDK 8
 * Maven >= 3.6.0

 1. First setup Hadoop 3.1.1 on the system with HDFS and YARN. Instructions for this are in [`HadoopSetup.md`](https://github.com/dream-lab/wicm/blob/main/HadoopSetup.md).
 2. Next, we install Graphite ICM jars, which is an extension of Apache Giraph. A single jar with all the dependencies is present under [`jars/`](https://github.com/dream-lab/wicm/tree/main/jars). To install ICM using maven:
```
cd jars
bash ./install.sh
```

Hadoop services should start on successful Hadoop/HDFS/YARN setup. Please see [`HadoopSetup.md`](https://github.com/dream-lab/wicm/blob/main/HadoopSetup.md) for details.
Successful installation of ICM will result in creation of `org/apache/giraph` under `~/.m2/repository`.


---
## 2. Building WICM from Source

Our WICM source code is present under [`src/`](https://github.com/dream-lab/wicm/tree/main/src/in/dreamlab/wicm). To build the project, run the `make.sh` script in the [`build/`](https://github.com/dream-lab/wicm/tree/main/build) folder.

```
cd build
bash ./make.sh
```

`WICM-1.0-SNAPSHOT-jar-with-dependencies.jar` will be created at the end of the build script under `build/`.

---
## 3. Running a Graphite ICM job

This evaluates the basline ICM platform that is used for comparison in our paper.

With Graphite ICM and Hadoop deployed, you can run your first ICM temporal graph processing job. We will use the **Earliest Arrival Time (EAT)** algorithm from the EuroSys paper for this example. The job reads an input file of an interval graph in one of the supported formats and computes the earliest arrival path from a provided source node. We will use `IntIntNullTextInputFormat` input format, which indicates that the vertex ID is of type `Int`, the time dimension is of type `Int`, with no (`Null`) edge properties, and `Text` implies that the input graph file is in text format. 

A sample graph [`sampleGraph.txt`](https://github.com/dream-lab/wicm/blob/main/build/graphs/sampleGraph.txt) has been provided in `build/graphs` with ~30,000 nodes ~1,000,000 edges. The spatial topology of the graph was generated using [`PaRMAT`](https://github.com/farkhor/PaRMAT). The start-time and end-time of interval edges are uniformly sampled. The lifespan of the vertex is set accordingly to maintain referential integrity in the graph.

Each line is an adjacency list of one source and one or more sink vertices of the format `source_id source_startTime source_endTime dest1_id dest1_startTime dest1_endTime dest2_id dest2_startTime dest2_endTime ...`. To run the `EAT` algorithm, the Giraph job script `runEAT.sh` has been provided in [`build/scripts/giraph/icm`](https://github.com/dream-lab/wicm/tree/main/build/scripts/giraph/icm). The job script takes 4 arguments:

 1. source : The source vertex ID from which the traversal algorithm will start (e.g., `0`)
 2. perfFlag : Set to `true` to dump performance related log information, `false` otherwise (e.g., `false`)
 3. inputGraph : HDFS path to the input graph (e.g., `sampleGraph.txt`)
 4. outputDir : HDFS path to the output folder (e.g., `output`)

```
runEAT.sh <source> <perfFlag> <inputGraph> <outputDir>
```

To run the script, first copy the sample graph file to HDFS:
```
hdfs dfs -copyFromLocal build/graphs/sampleGraph.txt
```
And check if the input graph has been copied to HDFS:
```
hdfs dfs -ls sampleGraph.txt
```

Running ICM mode job with sourceID as `0`:
```
cd build
bash ./scripts/giraph/icm/runEAT.sh 0 false sampleGraph.txt output
```
`output` should be present under `build/` after successful finishing of the job.

---
## 4. Running ICM with vertex-local optimizations (ICM+LU+DS)

This evaluates the Local Unrolling (LU) and Deferred Scatter (DS) optimizations proposed by us in the paper using the ICM baseline.

The related scripts are provided in [`build/scripts/giraph/icm_luds`](https://github.com/dream-lab/wicm/tree/main/build/scripts/giraph/icm_luds). The scripts have *additional arguments*, besides the 4 arguments for the ICM script above:

 5. bufferSize : Size of the message cache to be used in LU optimisation (e.g., `100`)
 6. minMsg : minimum message cardinality for LU optimisation (e.g., `20`)
```
runEAT.sh <source> <bufferSize> <minMsg> <perfFlag> <inputGraph> <outputDir>
```

To run ICM mode job with sourceID as `0`, buffersize of `100` and minMsg of `20`:
```
cd build
bash ./scripts/giraph/icm_luds/runEAT.sh 0 100 20 false sampleGraph.txt output
```
`output` should be present under `build/` after successful finishing of the job.

---
## 5. Running a WICM job 

This evaluates the Windowed ICM optimization proposed by us in the paper.

The related scripts are provided in [`build/scripts/giraph/wicm`](https://github.com/dream-lab/wicm/tree/main/build/scripts/giraph/wicm). The scripts have additional arguments compared to the ICM script:

5. lowerE : Start time of the graph lifespan (e.g., `0`)
6. upperE : End time of the graph lifespan (e.g., `40`)
7. windows : Temporal partitioning of the graph's lifespan, specified as timepoint boundaries separated by semicolon (e.g., `0;20;30;40`)

```
runEAT.sh <source> <lowerE> <upperE> <windows> <perfFlag> <inputGraph> <outputDir>
```

The sample graph `sampleGraph.txt` has a lifespan of [0,40). We assume some split strategy provides us the windows as [0,20), [20,30) and [30,40). Later, in #10, we describe the command to run the heuristics that will offer a more intelligent window partitions that ca be used here. 
To run the WICM job using this configuration and with the same source ID `0` on the sample graph:

```
cd build
bash ./scripts/runEAT.sh 0 0 40 "0;20;30;40" false sampleGraph.txt output
```
`output` should be present under `build/` after successful finishing of the job.

---
## 6. Running WICM with vertex-local optimizations (WICM+LU+DS)

This evaluates the Windowed ICM optimization, coupled with the Local Unrolling (LU) and Deferred Scatter (DS) optimizations, as proposed by us in the paper.

The related scripts are provided in [`build/scripts/giraph/wicm_luds`](https://github.com/dream-lab/wicm/tree/main/build/scripts/giraph/wicm_luds). The scripts have additional arguments compared to the ICM script:

5. lowerE : Start time of the graph lifespan (e.g., `0`)
6. upperE : End time of the graph lifespan (e.g., `40`)
7. windows : Temporal partitioning of the graph's lifespan, specified as timepoint boundaries separated by semicolon (e.g., `0;20;30;40`)
8. bufferSize : Size of the message cache to be used in LU optimisation (e.g., `100`)
9. minMsg : minimum message cardinality for LU optimisation (e.g., `20`)
```
runEAT.sh <source> <lowerE> <upperE> <windows> <buffersize> <minMsg> <perfFlag> <inputGraph> <outputDir>
```

The sample graph `sampleGraph.txt` has a lifespan of [0,40). We assume some split strategy provides us the windows as [0,20), [20,30) and [30,40). To run the WICM job using this configuration and with the same source ID `0` on the sample graph:

```
cd build
bash ./scripts/runEAT.sh 0 0 40 "0;20;30;40" 100 20 false sampleGraph.txt output
```
`output` should be present under `build/` after successful finishing of the job.

---
## 7. Running Other Graph Algorithms

Our paper evaluates six graph traversal algorithms: Earliest Arrival TIme (EAT), Single Source Shortest Path (SSSP), Temporal Reachability (TR), Latest Departure time (LD), Temporal Minimum Spanning Tree (TMST) and Fastest travel Time (FAST).
We have provided a job scripts for all platform variants to run each of these 6 traversal algorithms: `runEAT.sh, runSSSP.sh, runTR.sh, runLD.sh, runTMST.sh, runFAST.sh` under respective folders [ICM](https://github.com/dream-lab/wicm/tree/main/build/scripts/giraph/icm), [ICM+LU+DS](https://github.com/dream-lab/wicm/tree/main/build/scripts/giraph/icm_luds), [WICM](https://github.com/dream-lab/wicm/tree/main/build/scripts/giraph/wicm) and [WICM+LU+DS](https://github.com/dream-lab/wicm/tree/main/build/scripts/giraph/wicm_luds). 

The scripts can be edited to specify the number of workers using the argument `-w <num_workers>` and the number of threads per worker using the argument `giraph.numComputeThreads <num_threads>`. By default, we run on `1` worker and `1` thread per worker.

The number of workers is the number of machines in the cluster. For Hadoop deployment in a distributed mode, please check [`Hadoop Cluster Setup`](https://hadoop.apache.org/docs/r3.1.1/hadoop-project-dist/hadoop-common/ClusterSetup.html). The current [`HadoopSetup.md`](https://github.com/dream-lab/wicm/blob/main/HadoopSetup.md) sets up Hadoop in a pseudo-distributed mode with 1 worker. 

---
## 8. Minimal experimental pipeline to run all algorithms

A minimal experiment script has been provided [`build/scripts/giraph/runExperiments.sh`](https://github.com/dream-lab/wicm/blob/main/build/scripts/giraph/runExperiments.sh) to run all algorithms for two different source vertices (`22499` and `19862`) in the sample graph, for ICM, ICM+LU+DS, WICM and WICM+LU+DS. For WICM variants, we evaluate two different temporal partitioning of the graph (`"0;20;30;40"` and `"0;21;40"`). 
For each source vertex and algorithm, the `build/scripts/giraph/compare.sh` script automatically verifies that the job output returned by ICM is identical to the ones returned by our optimizations, ICM+LU+DS, WICM and WICM+LU+DS. This is a sanity check to ensure the correctness of our optimizations relative to the ICM baseline.

To run the experiment pipeline:

```
cd build
bash ./scripts/giraph/runExperiments.sh > experiment.out 2> experiment.err
```
The script should also create a file `experiment.log` with the table:
|            | EAT | SSSP | TR | TMST | LD | FAST |
|------------|-----|------|----|------|----|------|
| LU+DS      | 2   | 2    | 2  | 2    | 2  | 2    |
| WICM       | 4   | 4    | 4  | 4    | 4  | 4    |
| WICM+LU+DS | 4   | 4    | 4  | 4    | 4  | 4    |

Each cell in the table depicts the number of experiments for which the algorithm-configuration combination produced equivalent outputs to native ICM. We expect `2` for LU+DS since we have runs on 2 source vertices, and we expect `4` for WICM and its variant since we have runs for two window splits for each of the two source vertices.

---
## 9. Running WICM Heuristic for Obtaining Window Splits

This evaluates the heuristics we propose in the paper for finding good split points for the graph windows that is the used by WICM. The output of this can be used by the `windows` input parameter in the above WICM scripts.

The heuristics are implemented using Apache Spark and Python.

Additional pre-requisites:
 * Apache Spark 3.1.2
 * Python >= 2.7

Instructions for setting up Apache Spark are present in [`SparkSetup.md`](https://github.com/dream-lab/wicm/blob/main/SparkSetup.md). Hadoop should have been setup before running Spark using the instructions from above.

The *Pyspark code* has been provided for obtaining the timepoint edge distribution required for running the heuristic. This code is present in [`build/scripts/heuristic/getGraphTimepoint_spark.py`](https://github.com/dream-lab/wicm/blob/main/build/scripts/heuristic/getGraphTimepoint_spark.py). It uses the same input graph format as described above under `3. Running a Graphite ICM job`. The script takes 3 arguments:
 1. inputGraph : HDFS path to input graph (e.g., `sampleGraph.txt`)
 2. upperE : End time of the input graph's lifespan, with the assumption that the start time of the graph lifespan is `0` (e.g. `40`)
 3. outputFile : Local path to store the obtained edge distributions (e.g., `sampleGraph.bin`)

```
spark-submit --master yarn --num-executors 1 --executor-cores 1 --executor-memory 2G getGraphTimepoint_spark.py <inputGraph> <upperE> <outputFile>
```

To run this pyspark code on the input graph `sampleGraph.txt` which has a lifespan of `(0,40]` and store the output in `sampleGraph.bin` under `build/graphs/distribution` folder, we run:
```
cd build/scripts/heuristic
spark-submit --master yarn --num-executors 1 --executor-cores 1 --executor-memory 2G getGraphTimepoint_spark.py sampleGraph.txt 40 sampleGraph.bin
```

The code to run the heuristic for the windo splits using this edge distribution is available in [`build/scripts/heuristic/split.py`](https://github.com/dream-lab/wicm/blob/main/build/scripts/heuristic/split.py). The script takes the edge distribution binary file as input and prints the split strategy obtained by running the heuristic on the distribution on the console.
```
python split.py <edge_dist_file>
```

To run the script for the edge distribution returned above in `sampleGraph.bin`, run:
```
cd build/scripts/heuristic
python split.py ../../graphs/distributions/sampleGraph.bin
```
The output of the above-mentioned command is
```
Unscaled distribution (0.9199715190124998, '0;23;40')
Scaled distribution (0.907843090338, '0;21;40')
```

The output is a tuple `(`&beta;`, heuristic_window_partitions)`, where &beta; is the *additional message replication cost* as described in the paper and the `heuristic_window_partitions` is the window splits determined by the heuristics (without distribution pruning) for the graph. The `heuristic_window_partitions` output can be used as a replacement for the `windows` argument in #5 and #6 commands above.

---
## 10. Graphs Evaluated in the Paper

The paper evaluates six different graphs, which were downloaded from the following sources. 
 1. Reddit: https://www.cs.cornell.edu/~jhessel/projectPages/redditHRC.html
 2. WebUK: http://law.di.unimi.it/webdata/uk-union-2006-06-2007-05/
 3. MAG: https://www.microsoft.com/en-us/research/project/open-academic-graph/
 4. Twitter: http://twitter.mpi-sws.org/
 5. LDBC-8_9-FB: datagen-8_9-fb - https://graphalytics.org/datasets
 6. LDBC-9_0-FB: datagen-9_0-fb - https://graphalytics.org/datasets

These original graphs were pre-processed before being used as input to ICM and WICM frameworks in place of the `sampleGraph.txt`. The pre-processing converts these graphs to the expected formats and normalizes the lifespans, as described in the EuroSys paper. The pre-processed graphs are available at Zenodo under: [`https://zenodo.org/record/5937376`](https://zenodo.org/record/5937376), and can be directly used in the above scripts.

The pre-computed edge distribution files for all these graphs are present under [`build/graphs/distributions`](https://github.com/dream-lab/wicm/tree/main/build/graphs/distributions).



---
## Contact

For more information, please contact: **Animesh Baranawal <animeshb@iisc.ac.in>**, DREAM:Lab, Department of Computational and Data Sciences, Indian Institute of Science, Bangalore, India
