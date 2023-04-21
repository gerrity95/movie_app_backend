from motor.core import AgnosticDatabase, AgnosticCollection
from motor.motor_asyncio import AsyncIOMotorClient
from env_config import Config
from base.mongoclient import MongoClient
from base.rabbitmq_client import RabbitMqClient
from uuid import uuid4


class PingEvent:

  def event_type(cls) -> str:
    return cls.__name__

  def __init__(self):
    self.uuid = str(uuid4())
    self.message = 'PING'

class StatusException(Exception):
    """
    class to handle exceptions in the Status class
    """

class StatusClient:
    """
    A generic mongo client
    """

    def __init__(self) -> None:
        self.config = Config()
        self.mongo_client = MongoClient()
        self.rabbitmq_client = RabbitMqClient()

    async def ping(self) -> bool:
        try:
            print('Attempting to ping clients.')
            mongo = await self.mongo_client.node_db().list_collection_names()
            rmq = await self.ping_rmq()
            print(mongo)
            print(rmq)
            return "True", 200
        except Exception as error:
            print(f"Error validation app status: {error}")
            return str(error), 500

    async def ping_rmq(self):
      """
        A function to ping RabbitMQ
      """
      mq_ping_event = PingEvent()
      routing_key = 'RabbitMqPing'

      ping_event_queue, error = await self.rabbitmq_client.declare_queue(routing_key=routing_key, durable=True)
      if error:
        print(f'Error when declaring Ping queue: {error}')
        raise StatusException('Error declaring Rabbitmq Ping queue')
      error = await self.rabbitmq_client.publish(message=mq_ping_event, routing_key=routing_key)
      if error:
        print(f'Error when publishing to Ping queue: {error}')
        raise StatusException('Error publishing to Rabbitmq Ping queue')       

      result, error = await self.rabbitmq_client.consume_first(routing_key=routing_key, queue=ping_event_queue)
      if error:
        print(f'Error when consuming from Ping queue: {error}')
        raise StatusException('Error consuming from Rabbitmq Ping queue')
      
      print('Successfully processed event through RabbitMq Ping Queue');
      await self.rabbitmq_client.delete_queue(routing_key=routing_key);
      await self.rabbitmq_client.close()
      return True
