from s00_create_resources import Transaction, KafkaClient

if __name__ == "__main__":
    item_str = """trans_id,account_id,type,operation,amount,balance,k_symbol,fulldatewithtime,customer_id
    T00695247,A00002378,Credit,Credit in Cash,700,700,,2013-01-01T11:02:40,C00002873"""

    client = KafkaClient("localhost:19092")
    client.send(
        Transaction.read_line(item_str=item_str), topic_info=("small-transactions", "transactionId")
    )
