# arg1 = number of tasks
# arg2 = path of the dbsnp file
# arg3 = path of the dict file
$SPARK_HOME/bin/spark-submit --class "VarDensity" --master local[*] --driver-memory 32g ./target/scala-2.11/vardensity_2.11-1.0.jar $1 $2 $3
