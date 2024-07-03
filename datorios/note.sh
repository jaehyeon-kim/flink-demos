/opt/flink/examples/python/datastream/datorios
cat /opt/flink/examples/python/datastream/datorios/tumbling_count_window.py


docker exec 

./datorios.sh my-cluster start
./datorios.sh list
./datorios.sh my-cluster flink run /flink_jobs/CarData.jar
./datorios.sh my-cluster stop

./datorios.sh my-cluster flink run \
  -py /opt/flink/apps/tumbling_count_window.py
