mvn install:install-file \
   -Dfile=icm-giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
   -DgroupId=org.apache.giraph \
   -DartifactId=giraph-core \
   -Dversion=1.3.0-SNAPSHOT \
   -Dpackaging=jar \
   -DgeneratePom=true
