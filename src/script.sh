export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
../bin/hadoop com.sun.tools.javac.Main PlayaRest*.java
jar cf pr.jar PlayaRest*.class
hadoop fs -rm /user/hduser/output/*
hadoop fs -rmdir /user/hduser/output
hadoop fs -rm /user/hduser/input/*
hadoop fs -copyFromLocal playas.csv /user/hduser/input
../bin/hadoop jar pr.jar PlayaRest /user/hduser/input /user/hduser/output -files /opt/hadoop/jj/restauracion.csv -D 5
hadoop fs -cat /user/hduser/output/part-r-00000 | more
hadoop fs -copyToLocal /user/hduser/output/part-r-00000 .
