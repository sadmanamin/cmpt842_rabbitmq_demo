from pikaClient import BasicPikaClient
import sys

class BasicMessageSender(BasicPikaClient):

    def declare_queue(self, queue_name):
        print(f"Trying to declare queue({queue_name})...")
        try:           
            self.channel.queue_declare(queue=queue_name)
        except Exception as e:
            print(e)

    def declare_exchange(self, exchange):
        exchange_split = exchange.split(' ')
        exchange_name = exchange_split[0]
        if exchange_name == 'default':
            return ''
        else:
            print(f"Trying to declare exchange({exchange_name})...")
            try:
                self.channel.exchange_declare(
                    exchange=exchange_name,
                    exchange_type=exchange_split[1]
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

    print(' [*] Send your messages to queues. Pass arguments of EXCHANGE EXCHANGE_TYPE,QUEUE,MESSAGE. To exit, press CTRL+C')
    try:
        while True:
            instructions = input().split(',')
            exchange = basic_message_sender.declare_exchange(exchange=instructions[0])
            queue = instructions[1]
            message = bytes(instructions[2], 'utf-8')

            # Declare a queue
            basic_message_sender.declare_queue(queue)

            # Send a message to the queue.
            basic_message_sender.send_message(exchange=exchange, routing_key=queue, body=message)

    except Exception as e:
        # Close connections.
        print(e)
        basic_message_sender.close()

