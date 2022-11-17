from pikaClient import BasicPikaClient

class BasicMessageSender(BasicPikaClient):

    def declare_queue(self, queue_name):
        print(f"Trying to declare queue({queue_name})...")
        try:           
            self.channel.queue_declare(queue=queue_name)
        except Exception as e:
            print(e)

    def declare_exchange(self, exchange_name, exchange_type):
        try:
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
            )
        except Exception as e:
            print(e)   

    def bind_queue(self, exchange_name, queue_name, routing_key_pattern):
        try:
            self.channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key_pattern
            )
        except Exception as e:
            print(e) 


    def send_message(self, exchange, routing_key, body):
        channel = self.connection.channel()
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body
        )
        print(f"Sent message. Exchange: {exchange}, Routing Key: {routing_key}, Body: {body}")

    def close(self):
        self.channel.close()
        self.connection.close()




if __name__ == "__main__":

    # Initialize Basic Message Sender which creates a connection
    # and channel for sending messages.
    basic_message_sender = BasicMessageSender(
        rabbitmq_broker_id="b-cc9e7b61-23ab-40d0-99c8-4cbbcb3f7719",
        rabbitmq_user="sadmanamin",
        rabbitmq_password="sadmanamin2022",
        region="us-west-2"
    )

    basic_message_sender.declare_exchange(
        exchange_name='exc_topic',
        exchange_type='topic'
    )

    basic_message_sender.declare_queue('topic_queue_1')
    basic_message_sender.declare_queue('topic_queue_2')

    basic_message_sender.bind_queue(
        exchange_name='exc_topic',
        queue_name='topic_queue_1',
        routing_key_pattern='email.*'
    )

    basic_message_sender.bind_queue(
        exchange_name='exc_topic',
        queue_name='topic_queue_2',
        routing_key_pattern='payment.*'
    )




    print(' [*] Send your messages to queues. Pass arguments of ROUTING_KEY,MESSAGE. To exit, press CTRL+C')
    try:
        while True:
            instructions = input().split(',')
            print(instructions)
            routing_key = instructions[0]
            message = bytes(instructions[1], 'utf-8')

            # Send a message to the queue.
            basic_message_sender.send_message(
                exchange='exc_topic', 
                routing_key=routing_key, 
                body=message
            )

    except Exception as e:
        # Close connections.
        print(e)
        basic_message_sender.close()

