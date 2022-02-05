#!/bin/bash

source=$1
perfFlag=$2
inputGraph=$3
outputDir=$4

##### restart hadoop
:<<'END'
echo "Restarting Hadoop..."
$HADOOP_HOME/sbin/stop-all.sh
sleep 10
$HADOOP_HOME/sbin/start-all.sh
sleep 10
echo "Hadoop restarted!"
sleep 40
END
echo "Starting ICM..."

hadoop jar WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.icm.SSSP \
--yarnjars WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.graphite.io.formats.IntDoubleNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphite.io.formats.IntDoubleIdWithValueTextOutputFormat -op $outputDir -w 8 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntDoubleIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntDoubleIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntDoubleIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.wicm.warpOperation.DoubleMin \
-ca giraph.numComputeThreads=14 \
-ca sourceId=$source \
-ca debugPerformance=$perfFlag

##### dump output
#hdfs dfs -copyToLocal $outputDir .
hdfs dfs -rm -r $outputDir

##### dump logs
appID=$(yarn app -list -appStates FINISHED,KILLED | grep "SSSP" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "SSSP_"$outputDir"_"$source"_debug.log"

##### sort output for efficient diff
#echo "Sorting debug output..."
#cat $outputDir/part* >> $outputDir/output.txt
#rm $outputDir/part*
#sort -k1 -n < $outputDir/output.txt > $outputDir/sorted.txt
#rm $outputDir/output.txt
