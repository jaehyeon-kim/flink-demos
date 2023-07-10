## S3
docker exec -it kafka-0 bash
cd /opt/bitnami/kafka/bin/

docker run --rm -it --network kafka-network bitnami/kafka:2.8.1 \
  /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-0:9092 --create --topic product_sales

docker run --rm -it --network kafka-network bitnami/kafka:2.8.1 \
  /opt/bitnami/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-0:9092 --topic product_sales

{"seller_id": "LNK", "product": "Toothbrush", "quantity": 22, "product_price": 3.99, "sales_date": "2021-07-01"}
{"seller_id": "LNK", "product": "Dental Floss", "quantity": 17, "product_price": 1.99, "sales_date": "2021-07-01"}
{"seller_id": "LNK", "product": "Toothpaste", "quantity": 8, "product_price": 4.99, "sales_date": "2021-07-01"}
{"seller_id": "OMA", "product": "Toothbrush", "quantity": 29, "product_price": 3.99, "sales_date": "2021-07-01"}
{"seller_id": "OMA", "product": "Toothpaste", "quantity": 9, "product_price": 4.99, "sales_date": "2021-07-01"}
{"seller_id": "OMA", "product": "Dental Floss", "quantity": 23, "product_price": 1.99, "sales_date": "2021-07-01"}
{"seller_id": "LNK", "product": "Toothbrush", "quantity": 25, "product_price": 3.99, "sales_date": "2021-07-02"}
{"seller_id": "LNK", "product": "Dental Floss", "quantity": 16, "product_price": 1.99, "sales_date": "2021-07-02"}
{"seller_id": "LNK", "product": "Toothpaste", "quantity": 9, "product_price": 4.99, "sales_date": "2021-07-02"}
{"seller_id": "OMA", "product": "Toothbrush", "quantity": 32, "product_price": 3.99, "sales_date": "2021-07-02"}
{"seller_id": "OMA", "product": "Toothpaste", "quantity": 13, "product_price": 4.99, "sales_date": "2021-07-02"}
{"seller_id": "OMA", "product": "Dental Floss", "quantity": 18, "product_price": 1.99, "sales_date": "2021-07-02"}
{"seller_id": "LNK", "product": "Toothbrush", "quantity": 20, "product_price": 3.99, "sales_date": "2021-07-03"}
{"seller_id": "LNK", "product": "Dental Floss", "quantity": 15, "product_price": 1.99, "sales_date": "2021-07-03"}
{"seller_id": "LNK", "product": "Toothpaste", "quantity": 11, "product_price": 4.99, "sales_date": "2021-07-03"}
{"seller_id": "OMA", "product": "Toothbrush", "quantity": 31, "product_price": 3.99, "sales_date": "2021-07-03"}
{"seller_id": "OMA", "product": "Toothpaste", "quantity": 10, "product_price": 4.99, "sales_date": "2021-07-03"}
{"seller_id": "OMA", "product": "Dental Floss", "quantity": 21, "product_price": 1.99, "sales_date": "2021-07-03"}

docker run --rm -it --network kafka-network bitnami/kafka:2.8.1 \
  /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-0:9092 --topic product_sales --from-beginning

## S4
docker run --rm -it --network kafka-network bitnami/kafka:2.8.1 \
  /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-0:9092 --create --topic sales_items