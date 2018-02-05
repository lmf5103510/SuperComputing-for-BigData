$SPARK_HOME/bin/spark-submit --class "StreamingMapper" --master local[*] --driver-memory 32g target/scala-2.11/StreamingMapper-assembly-0.1-SNAPSHOT.jar
