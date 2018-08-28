#!/bin/sh
source ../login.sh
spark-submit --master yarn-cluster --class framework.spark.generateTableHangzhou --driver-class-path /app/catt/service/jars/ojdbc7.jar --jars
/app/catt/service/jars/ojdbc7.jar --num-executors 300 --executor-cores 1 --driver-memory 6g --executor-memory 8g aliveble.jar 2017122600 570