../bin/hadoop com.sun.tools.javac.Main PlayaRest*.java
jar cf pr.jar PlayaRest*.class
hadoop fs -rm /user/hduser/output/*
hadoop fs -rmdir /user/hduser/output
../bin/hadoop jar pr.jar PlayaRest /user/hduser/input /user/hduser/output -files /opt/hadoop/jj/restauracion.csv -D 5
