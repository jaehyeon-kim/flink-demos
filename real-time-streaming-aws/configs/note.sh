curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @configs/sink.json

curl http://localhost:8083/connectors/real-time-streaming-taxi-rides-sink/status

curl -X DELETE http://localhost:8083/connectors/real-time-streaming-taxi-rides-sink

aws dynamodb create-table --cli-input-json file://configs/ddb.json
aws dynamodb delete-table --table-name real-time-streaming-taxi-rides