from pikaClient import BasicPikaClient
import sys

class BasicMessageReceiver(BasicPikaClient):

    def declare_queue(self, queue_name):
        print(f"Trying to declare queue({queue_name})...")
        try:           
            self.channel.queue_declare(queue=queue_name)
        except Exception as e:
            print(e)

    def get_message(self, queue):
        method_frame, header_frame, body = self.channel.basic_get(queue)
        if method_frame:
            print(method_frame, header_frame, body)
            self.channel.basic_ack(method_frame.delivery_tag)
            return method_frame, header_frame, body
        else:
            print('No message returned')

    def consume_messages(self, queue):
        def callback(ch, method, properties, body):
            print(" [x] Received %r" % body.decode('utf-8'))

        self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def close(self):
        self.channel.close()
        self.connection.close()


if __name__ == "__main__":

    queue = sys.argv[1]

    # Create Basic Message Receiver which creates a connection
    # and channel for consuming messages.
    basic_message_receiver = BasicMessageReceiver(
        rabbitmq_broker_id="b-cc9e7b61-23ab-40d0-99c8-4cbbcb3f7719",
        rabbitmq_user="sadmanamin",
        rabbitmq_password="sadmanamin2022",
        region="us-west-2"
    )

    # Consume the message that was sent.
    # basic_message_receiver.get_message("hello world queue")
    basic_message_receiver.declare_queue(queue)
    basic_message_receiver.consume_messages(queue)
    # Close connections.
    basic_message_receiver.close()