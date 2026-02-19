import asyncio
import json
from bson import json_util
from typing import Optional
from base.events import RecommendationsEvent, State
from base.rabbitmq_client import RabbitMqClient
from recommendations import Recommendations
import traceback
from aio_pika import IncomingMessage
from aio_pika.robust_queue import RobustQueueIterator


class AsyncRMQ:

    def __init__(self) -> None:
        self.rabbitmq_client = RabbitMqClient()
        self.recommendations = Recommendations()
        self.iterator: Optional[RobustQueueIterator] = None

    async def consume_reccs_events(self):
        while True:
            try:
                routing_key = RecommendationsEvent.routing_key()
                events_queue, error = await self.rabbitmq_client.declare_queue(routing_key=routing_key,
                                                                               durable=True,
                                                                               auto_delete=False)
                if error:
                    print(f"Error {error} attempting to declare the queue for routing key: {routing_key}")
                    recommendations_event.state = State.fail
                    raise error

                await self.rabbitmq_client.refresh_channel()
                async with events_queue.iterator() as iterator:
                    self.iterator = iterator
                    async for message in iterator:
                        message: IncomingMessage = message
                        async with message.process():
                            try:
                                print("MESSAGE")
                                print(message)
                                event_dict: dict = json.loads(message.body, object_hook=json_util.object_hook)
                                recommendations_event: RecommendationsEvent = RecommendationsEvent.reconstruct(event_dict)
                                print(
                                    f"Consumed RecommendationsEvent for user: {recommendations_event.user_id}")
                                recommendations_event.state = State.in_progress
                            except Exception as err:
                                print(f"Error attempting to ingest message from RMQ -> {err}")
                                raise err
                            new_reccs, error = await self.recommendations.process_recommendations(user_id=recommendations_event.user_id)
                            if error:
                                print(f"Error {error} calculating reccs for user: {recommendations_event.user_id}")
                                recommendations_event.state = State.fail
                            else:
                                print(f"Successfully calculated Recommendations for user: {recommendations_event.user_id}")
                                recommendations_event.state = State.ok
                                recommendations_event.reccomendations = new_reccs

                            if message.correlation_id:
                                print(f"Returning RecommendationsEvent for {recommendations_event.user_id}")

                                exception_new = await self.rabbitmq_client.publish_new(message=recommendations_event.deconstruct(),
                                                                                    correlation_id=message.correlation_id,
                                                                                    routing_key=message.reply_to, default=True)
                                if exception_new:
                                    print(f"Error {exception_new} when attempting to send recommendations event back to the reply queue")
                                else:
                                    print(f"Published RecommendationsEvent back to it's source")
                            else:
                                print(f"No correlation ID. Not publishing back to reply queue.")

            except Exception as error:
                print(
                    f"Failure seen attempting to consume RecommendationEvents: {error}. Sleeping for 30 seconds")
                print(traceback.format_exc())
                await asyncio.sleep(30)


def main():
    app = AsyncRMQ()
    try:
        asyncio.run(app.consume_reccs_events())
    except Exception as e:
        print(f"An error occurred: {e}")
    # try:
    #     loop = asyncio.get_event_loop()
    #     loop.run_until_complete(asyncio.gather(app.consume_reccs_events()))
    # finally:
    #     loop.close()


if __name__ == "__main__":
    main()
