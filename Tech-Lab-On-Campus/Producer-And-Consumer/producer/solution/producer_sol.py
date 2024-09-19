import pika
import os

from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):

    def __init__(self, routing_key, exchange_name) -> None:
        # Save parameters to instance variable
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        self.connection = connection
        channel = self.connection.channel()
        exchange = channel.exchange_declare(exchange=self.exchange_name)
        channel.basic_publish(
            self.exchange_name,
            self.routing_key,
            body="Message",
        )
    
    def publishOrder(self, message: str) -> None:
        channel = self.connection.channel()
        channel.basic_publish(
            exchange="Exchange Name",
            routing_key="Routing Key",
            body=message,
        )
        


