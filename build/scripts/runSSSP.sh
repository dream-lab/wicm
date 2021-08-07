#!/bin/bash

mode=$1

############################################################ Window ICM ############################################################
if [[ "$mode" == "WICM" ]]; then
source=$2
lowerE=$3
upperE=$4
windows="$5"
perfFlag=$6
inputGraph=$7
outputDir=$8

##### restart hadoop
:<<'END'
echo "Restarting YARN..."
$HADOOP_HOME/sbin/stop-yarn.sh
sleep 10
$HADOOP_HOME/sbin/start-yarn.sh
sleep 10
echo "YARN restarted!"
sleep 40
END
echo "Starting WICM job..."

hadoop jar WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.wicm.SSSP \
--yarnjars WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 3000 \
-vif in.dreamlab.graphite.io.formats.IntDoubleNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphite.io.formats.IntDoubleIdWithValueTextOutputFormat -op $outputDir"_windowed" -w 1 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntDoubleIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntDoubleIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntDoubleIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.wicm.warpOperation.DoubleMin \
-ca giraph.masterComputeClass=in.dreamlab.wicm.graph.computation.GraphiteIntCustomWindowMaster \
-ca giraph.workerContextClass=in.dreamlab.wicm.graph.computation.GraphiteDebugWindowWorkerContext \
-ca giraph.numComputeThreads=3 \
-ca sourceId=$source \
-ca lowerEndpoint=$lowerE \
-ca upperEndpoint=$upperE \
-ca windows="$windows" \
-ca debugPerformance=$perfFlag

##### dump output
hdfs dfs -copyToLocal $outputDir"_windowed" .
hdfs dfs -rm -r $outputDir"_windowed"

##### dump logs
# appID=$(yarn app -list -appStates FINISHED,KILLED | grep "SSSP" | sort -k1 -n | tail -n 1 | awk '{print $1}')
# echo $appID
# yarn logs -applicationId $appID > "SSSP_"$outputDir"_"$source"_window.log"

##### sort output for efficient diff
echo "Sorting windowed output..."
cat $outputDir"_windowed"/part* >> $outputDir"_windowed"/output.txt
rm $outputDir"_windowed"/part*
sort -k1 -n < $outputDir"_windowed"/output.txt > $outputDir"_windowed"/sorted.txt
rm $outputDir"_windowed"/output.txt

############################################################ Window ICM ############################################################
elif [[ "$mode" == "ICM" ]]; then
source=$2
perfFlag=$3
inputGraph=$4
outputDir=$5

##### restart hadoop
:<<'END'
echo "Restarting YARN..."
$HADOOP_HOME/sbin/stop-yarn.sh
sleep 10
$HADOOP_HOME/sbin/start-yarn.sh
sleep 10
echo "YARN restarted!"
sleep 40
END
echo "Starting ICM job..."

hadoop jar WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.icm.SSSP \
--yarnjars WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 3000 \
-vif in.dreamlab.graphite.io.formats.IntDoubleNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphite.io.formats.IntDoubleIdWithValueTextOutputFormat -op $outputDir"_debug" -w 1 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntDoubleIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntDoubleIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntDoubleIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.wicm.warpOperation.DoubleMin \
-ca giraph.numComputeThreads=3 \
-ca sourceId=$source \
-ca debugPerformance=$perfFlag

##### dump output
hdfs dfs -copyToLocal $outputDir"_debug" .
hdfs dfs -rm -r $outputDir"_debug"

##### dump logs
# appID=$(yarn app -list -appStates FINISHED,KILLED | grep "SSSP" | sort -k1 -n | tail -n 1 | awk '{print $1}')
# echo $appID
# yarn logs -applicationId $appID > "SSSP_"$outputDir"_"$source"_debug.log"

##### sort output for efficient diff
echo "Sorting debug output..."
cat $outputDir"_debug"/part* >> $outputDir"_debug"/output.txt
rm $outputDir"_debug"/part*
sort -k1 -n < $outputDir"_debug"/output.txt > $outputDir"_debug"/sorted.txt
rm $outputDir"_debug"/output.txt

elif [[ "$mode" == "compare" ]]; then
ICMD="$2"
WICMD="$3"
diff "$ICMD"/sorted.txt "$WICMD"/sorted.txt > diff.txt

if [ -s diff.txt ]; then
        rm diff.txt
        echo "Not equivalent"
        exit 1
else
        rm diff.txt
        echo "Equivalent"
        exit 0
fi

fi
