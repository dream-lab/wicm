#!/bin/bash

source=$1
perfFlag=$2
inputGraph=$3
outputDir=$4

:<<'END'
echo "Restarting hadoop..."
$HADOOP_HOME/sbin/stop-all.sh
sleep 10
$HADOOP_HOME/sbin/start-all.sh
sleep 10
echo "hadoop restarted!"
sleep 40
END
echo "Starting ICM job..."

### default
hadoop jar WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.icm.REACH_D \
--yarnjars WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 3000 \
-vif in.dreamlab.graphite.io.formats.IntBooleanNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphite.io.formats.IntBooleanIdWithValueTextOutputFormat -op $outputDir -w 1 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntBooleanIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntBooleanIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntBooleanIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.BooleanOr \
-ca giraph.numComputeThreads=1 \
-ca sourceId=$source \
-ca debugPerformance=$perfFlag

hdfs dfs -copyToLocal $outputDir .
hdfs dfs -rm -r $outputDir

# appID=$(yarn app -list -appStates FINISHED,KILLED | grep "REACH_D" | sort -k1 -n | tail -n 1 | awk '{print $1}')
# echo $appID
# yarn logs -applicationId $appID > "TR_"$outputDir"_"$source"_debug.log"

echo "Sorting windowed output..."
cat $outputDir/part* >> $outputDir/output.txt
rm $outputDir/part*
sort -k1 -n < $outputDir/output.txt > $outputDir/sorted.txt
rm $outputDir/output.txt