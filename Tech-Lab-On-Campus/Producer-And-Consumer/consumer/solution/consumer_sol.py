import pika
import os
from consumer_interface import mqConsumerInterface


class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name):
        self.binding_key = binding_key
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.setupRMQConnection()
    
    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:

        print("Heres the body: " + str(body))

    
    def setupRMQConnection (self):
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
        self.channel.queue_bind(queue= self.queue_name,
        routing_key= self.binding_key,
        exchange=self.exchange_name,
        )
        

    def startConsuming(self) -> None:
        print(" [*] Waiting for messages. To exit press CTRL+C")
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(queue=
        self.queue_name,on_message_callback= self.on_message_callback, auto_ack=False
        )

    
    def __del__(self) -> None:
        print("Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()


        
