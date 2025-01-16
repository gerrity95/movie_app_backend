from datetime import datetime
from typing import Any, Dict, List, Tuple
import asyncio
import json
import backoff
from bson import json_util
import aio_pika
from aio_pika import Queue, IncomingMessage
from aio_pika.abc import AbstractRobustConnection
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
        self.exchange_name = "whattowatch-exchange"
        self.exchange = None
    
    async def connect(self) -> AbstractRobustConnection:
        """
        This function returns a connection for RMQ
        """
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

    async def refresh_channel(self):
        """
        This function refreshes the connection and channel
        :raises: IOError when re-establishing the connection to RMQ fails
        """
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

    async def declare_queue(self, routing_key, durable, auto_delete=False):
        try:
            await self.refresh_connection()
            queue_name = f"{self.exchange_name}.{routing_key}"

            queue = await self.channel.declare_queue(name=queue_name, timeout=self.timeout, durable=durable, auto_delete=auto_delete)
            await queue.bind(exchange=self.exchange, routing_key=routing_key)
            print(f"Successfully declared queue: {queue_name}")
            return queue, None
        except Exception as error:
            print(
                f'Error {error} attempting to declare queue for {routing_key}')
            return None, error

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

    async def publish_new(self, message: dict, routing_key: str) -> Exception:
        """
        This is a function design that will allow us to publish a message to a RMQ channel
        :param message: The message body
        :param routing_key: The queue in which to publish the message to
        """

        try:
            await self._publish_with_retries(message=message, routing_key=routing_key)
        except Exception as error:
            print(f"Failed to publish to RMQ -> {error}")
            return error
    
    @backoff.on_exception(backoff.fibo, Exception, max_tries=3, max_time=60)
    async def _publish_with_retries(self, message: dict, routing_key: str) -> None:
        await self.refresh_channel()
        print(f"Publishing message {message.get('uuid')} to {routing_key}")
        body = json.dumps(message, default=json_util.default)
        body_as_bytes = body.encode()
        message = aio_pika.Message(body=body_as_bytes, expiration=60)
        await self.exchange.publish(message=message,
                                    routing_key=routing_key,
                                    timeout=self.timeout)
    
    async def generator(self, queue: Queue, ignore_processed: bool = False, timeout: int = None):
        """
        Async generator design to help facilitate the consuming of messages when using multiple consumers
        """
        await self.refresh_channel()
        async with queue.iterator(timeout=timeout) as iterator:
            async for message in iterator:
                async with message.process(ignore_processed=ignore_processed):
                    message: IncomingMessage = message
                    event: dict = json.loads(message.body, object_hook=json_util.object_hook)
                    print(f"Consume messsage {event.get('uuid')} with size of {message.body_size}")
                    
                    yield message, event


    async def consume(self, routing_key: str, queue: Queue, count: int = float("inf"), timeout: int = 60) -> Tuple[List[Dict], Exception]:
        """
        Allows us to consume and parse a message from a RMQ Queue
        """
        return await self._consume(routing_key=routing_key, queue=queue, count=count, timeout=timeout)

    async def _consume(self, routing_key: str, queue: Queue, count: int = float("inf"), timeout: int = 60) -> Tuple[List[Dict], Exception]:
        """
        This is a private function designed to pull messages from a rabbitmq queue
        Returns a list of messages or an exception
        """
        events = []
        try:
            await self.refresh_channel()
            async for message, event in self.generator(queue=queue, timeout=timeout):
                event: dict = event
                events.append(event)
                if (len(events)) == count:
                    print(f"Stop consuming messages\nConsumed count: {len(events)}\nTarget Count: {count}")
                    break
                else:
                    print(f"Waiting for more messages\nConsumed count: {len(events)}\nTarget Count: {count}")
            
            return events, None
        except Exception as error:
            if events:
                event: Dict = events[0]
                print(f"Consuming {event.get('uuid')} was partially successful. Consumed count: {len(events)}\nTarget Count: {count}")
                return events, None
            print(f"Failed to consume from MQ {routing_key} with error: {error}")
            return None, error


        # await self.refresh_connection()
        # events = []
        # try:
        #     async for event in self.consume(queue=queue):
        #         events.append(event)
        #         if len(events) == count:
        #             print(
        #                 f"Stopping consuming for {event.__class__.__name__}. Consumed count: {len(events)}")
        #             break
        #         else:
        #             print(
        #                 f"Waiting for more events for {event.__class__.__name__}")
        #     return events, None
        # except Exception as err:
        #     if events:
        #         print(
        #             f"Consuming {event.__class__.__name__} was partially successful.")
        #         return events, None
        #     else:
        #         print(
        #             f"Failed to consume events from RMQ {routing_key} with error: {err}")
        #         return None, err

    async def consume_first(self, routing_key: str, queue: Queue, count: int = 1, timeout: int = 60):
        """
        This is a convenience function that returns a single message/event from RMQ
        """
        result, error = await self.consume(routing_key=routing_key, queue=queue, count=count, timeout=timeout)
        if result:
            return result[0], None
        return None, error