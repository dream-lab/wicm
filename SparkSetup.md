# Setting Apache Spark 3.1.2

Tested on Linux Ubuntu-based system with >= 8GB RAM.

0. Ensure that Hadoop is properly set.

1. Download the binary: https://dlcdn.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-without-hadoop.tgz

2. Extract it at location of choice (DIR).
   Export spark-related variables and update the environment `PATH` variable.
```
export SPARK_HOME=/home/animeshbaranawal/Desktop/spark-3.1.2-bin-without-hadoop
export PATH=$PATH:$SPARK_HOME/bin
```

3. Create copy of DIR/spark-3.1.2-bin-without-hadoop/conf/spark-env.sh.template as DIR/spark-3.1.2-bin-without-hadoop/conf/spark-env.sh.
   Add the line `export SPARK_DIST_CLASSPATH=$(hadoop classpath)` to DIR/spark-3.1.2-bin-without-hadoop/conf/spark-env.sh