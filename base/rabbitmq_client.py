from datetime import datetime
from typing import Any
import asyncio
import aio_pika
import pickle
from env_config import Config


class RabbitMqClient:

    def __init__(self) -> None:
        self.config = Config()
        self.endpoint = self.config.RMQ_HOST
        self.port = self.config.RMQ_PORT
        self.user = self.config.RMQ_USER
        self.password = self.config.RMQ_PASSWORD
        self.timeout = 60
        self.connection = None
        self.channel = None
        self.exchange_name = "fmovies-exchange"
        self.exchange = None
        
    async def connect(self):
        print("Connecting to RMQ")
        connection = await aio_pika.connect_robust(url=f"amqp://{self.user}:{self.password}@{self.endpoint}:{self.port}/",
                                                   timeout=self.timeout)
        return connection
    
    async def refresh_connection(self):
        if not self.channel or self.channel.is_closed:
            if not self.connection or self.connection.is_closed:
                print("Refreshing RMQ Connection")
                self.connection = await self.connect()
                print("Refreshing Channel connection")
                self.channel = await self.connection.channel()
                self.exchange = await self.channel.declare_exchange(name=self.exchange_name, durable=True)
                print(f"RabbitMQ Exchange {self.exchange} is declared")
                
    async def close(self):
        """
        This function closes the connection and channel
        """
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
            print("RabbitMQ Client close channel")
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            print("RabbitMQ Client close connection")
            
    async def declare_queue(self, routing_key, durable, auto_delete):
        await self.refresh_connection()
        queue_name = f"{self.exchange_name}.{routing_key}"
        
        queue = await self.channel.declare_queue(name=queue_name, timeout=self.timeout, durable=durable, auto_delete=auto_delete)
        await queue.bind(exchange=self.exchange, routing_key=routing_key)
        print(f"Successfully declared queue: {queue_name}")
        return queue
    
    async def delete_queue(self, routing_key):
        await self.refresh_connection()
        queue_name = f"{self.exchange_name}.{routing_key}"
        print(f"Attempting to delete queue: {queue_name}")
        
        await self.channel.queue_delete(queue_name=queue_name, timeout=self.timeout)
        print(f"Successfully deleted queue: {queue_name}")
    
    async def ping(self):
        # TODO VERIFY THIS FUNCTION
        ping = await self.connect()
        
        return "True", 200
    
    async def publish(self, message: Any, routing_key: str):
        """
        Allows us to publish a message to a RMQ Queue
        """
        await self.refresh_connection()
        print(f"Publish to MQ {message.__class__.__name__} {message.uuid}")
        body = pickle.dumps(message)
        message = aio_pika.Message(body=body, expiration=60)
        await self.exchange.publish(message=message, routing_key=routing_key, timeout=self.timeout)
        
    async def consume(self, queue):
        """
        Allows us to consume and parse a message from a RMQ Queue
        """
        await self.refresh_connection()
        async with queue.iterator() as iterator:
            async for message in iterator:
                async with message.process():
                    event = pickle.loads(message.body)
                    print(f"Consume from MQ {event.__class__.__name__} {event.uuid}")
                    yield event
                    try:
                        if queue.name in message.body.decode():
                            break
                    except Exception as e:
                        pass
                    
    async def _consume(self, routing_key, queue, count):
        """
        Will consume the number of events specified in count from a given queue
        """
        await self.refresh_connection()
        events = []
        try:
            async for event in self.consume(queue=queue):
                events.append(event)
                if len(events) == count:
                    print(f"Stopping consuming for {event.__class__.__name__}. Consumed count: {len(events)}")
                    break
                else:
                    print(f"Waiting for more events for {event.__class__.__name__}")
            return events, None
        except Exception as err:
            if events:
                print(f"Consuming {event.__class__.__name__} was partially successful.")
                return events, None
            else:
                print(f"Failed to consume events from RMQ {routing_key} with error: {err}")
                return None, err
            
    async def consume_first(self, routing_key, queue, count):
        
        start = datetime.now()
        try:
            return await asyncio.wait_for(self._consume(routing_key=routing_key,
                                                        queue=queue,
                                                        count=count),
                                          timeout=20)
        except asyncio.futures.TimeoutError as error:
            elapsed = (datetime.now() - start).total_seconds()
            print(f"Consuming events has timed out after {elapsed} seconds")
            return None, error
