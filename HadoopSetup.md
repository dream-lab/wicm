# Setting Hadoop 3.1.1 
## *Pseudo-distributed mode*

Tested on Linux Ubuntu-based system with >= 8GB RAM.

1. Download the binary: https://archive.apache.org/dist/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz
   The version is important.

2. Extract it at location of choice (DIR).
   
   Create Namenode(NAMENODEDIR) and Datanode(DATANODEDIR) directories.
   
   Make sure that passwordless ssh to localhost is enabled.


3. Export the following variables to `.bashrc`
```
export HADOOP_HOME=<absolute path to DIR/hadoop-3.1.1>
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

4. Update the environment `PATH` variable : `export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin`

5. Add `export JAVA_HOME=<absolute path to JAVA_HOME>` to `DIR/hadoop-3.1.1/etc/hadoop/hadoop-env.sh`.

6. Make the following changes to files in `DIR/hadoop-3.1.1/etc/hadoop`:
    - `core-site.xml`
    ```
    <configuration>
    <property>
      <name>hadoop.tmp.dir</name>
      <value>/tmp</value>
    </property>
    <property>
      <name>fs.default.name</name>
      <value>hdfs://localhost:9000</value>
    </property>
    </configuration>
    ```
    - `hdfs-site.xml`
    ```
    <configuration>
    <property>
     <name>dfs.replication</name>
     <value>1</value>
    </property>
    <property>
      <name>dfs.name.dir</name>
      <value>file://<absolute path to NAMENODEDIR></value>
    </property>
    <property>
      <name>dfs.data.dir</name>
      <value>file://<absolute path to DATANODEDIR></value>
    </property>
    </configuration>
    ```
    - `mapred-site.xml`
    ```
    <configuration>
      <property>
        <name>mapreduce.job.user.name</name>
        <value><username></value>
      </property>
      <property>
        <name>yarn.resourcemanager.address</name>
        <value>localhost:8032</value>
      </property>
      <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
      </property>
      <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=<absolute path to DIR/hadoop-3.1.1></value>
      </property>
      <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=<absolute path to DIR/hadoop-3.1.1></value>
      </property>
      <property>
         <name>mapreduce.reduce.env</name>
         <value>HADOOP_MAPRED_HOME=<absolute path to DIR/hadoop-3.1.1></value>
      </property>
      <property>
        <name>mapred.tasktracker.map.tasks.maximum</name>
        <value>1</value>
      </property>
      <property>
        <name>mapred.map.tasks</name>
        <value>1</value>
      </property>
    </configuration>
    ```
    - `yarn-site.xml`
    ```
    <configuration>
     <property>
      <name>yarn.resourcemanager.hostname</name>
      <value>localhost</value>
     </property>
     <property>
      <name>yarn.server.resourcemanager.application.expiry.interval</name>
      <value>60000</value>
     </property>
     <property>
      <name>yarn.nodemanager.aux-services</name>
      <value>mapreduce_shuffle</value>
     </property>
      <property>
         <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
         <value>org.apache.hadoop.mapred.ShuffleHandler</value>
       </property>
      <property>
         <name>yarn.log-aggregation-enable</name>
         <value>true</value>
       </property>
      <property>
         <name>yarn.log-aggregation.retain-seconds</name>
         <value>-1</value>
       </property>
      <property>
         <name>yarn.application.classpath</name>
       <value>$HADOOP_CONF_DIR,${HADOOP_COMMON_HOME}/share/hadoop/common/*,${HADOOP_COMMON_HOME}/share/hadoop/common/lib/*,${HADOOP_HDFS_HOME}/share/hadoop/hdfs/*,${HADOOP_HDFS_HOME}/share/hadoop/hdfs/lib/*,${HADOOP_MAPRED_HOME}/share/hadoop/mapreduce/*,${HADOOP_MAPRED_HOME}/share/hadoop/mapreduce/lib/*,${HADOOP_YARN_HOME}/share/hadoop/yarn/*,${HADOOP_YARN_HOME}/share/hadoop/yarn/lib/*</value>
       </property>
       <property>
            <name>yarn.nodemanager.env-whitelist</name>
            <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
        </property>
        <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
        <description>Whether virtual memory limits will be enforced for containers</description>
     </property>
     <property>
       <name>yarn.nodemanager.vmem-pmem-ratio</name>
        <value>2.1</value>
        <description>Ratio between virtual memory to physical memory when setting memory limits for containers</description>
      </property>
     <property>
       <name>yarn.nodemanager.resource.memory-mb</name>
       <value>7168</value>
     </property>
     <property>
       <name>yarn.scheduler.maximum-allocation-mb</name>
       <value>7168</value>
     </property>
     <property>
       <name>yarn.webapp.ui2.enable</name>
       <value>true</value>
     </property>
     <property>
       <name>yarn.scheduler.minimum-allocation-vcores</name>
       <value>1</value>
     </property>
     <property>
       <name>yarn.scheduler.maximum-allocation-vcores</name>
       <value>1</value>
     </property>
     <property>
       <name>yarn.nodemanager.resource.cpu-vcores</name>
       <value>1</value>
     </property>
    </configuration>
    ```
    - `workers`
    ```
    localhost
    ```
7. Format namenode - `hdfs namenode -format`
8. 
   Start hadoop + YARN services - `start-all.sh`
   
   Use `jps` to check if services are up. The output should look like
   ```
   <pid> NameNode
   <pid> SecondaryNameNode
   <pid> DataNode
   <pid> ResourceManager
   <pid> NodeManager
   ```
   The SecondaryNameNode service is optional.
   In case, you receive the error: `localhost: rcmd: socket: Permission denied`, please see https://stackoverflow.com/questions/42756555/permission-denied-error-while-running-start-dfs-sh to resolve the issue. 

8. Create hdfs directory for self-username: `hdfs dfs -mkdir -p /user/<username>/`
