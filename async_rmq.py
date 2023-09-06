import asyncio
from base.events import RecommendationsEvent, State
from base.rabbitmq_client import RabbitMqClient
from recommendations import Recommendations
import traceback


class AsyncRMQ:

    def __init__(self) -> None:
        self.rabbitmq_client = RabbitMqClient()
        self.recommendations = Recommendations()

    async def consume_reccs_events(self):
        while True:
            try:
                routing_key = RecommendationsEvent.routing_key()
                events_queue, error = await self.rabbitmq_client.declare_queue(routing_key=routing_key,
                                                                               durable=True,
                                                                               auto_delete=False)

                mq_consumer = self.rabbitmq_client.consume(queue=events_queue)
                async for event in mq_consumer:
                    recommendations_event: RecommendationsEvent = event
                    print(
                        f"Consumed RecommendationsEvent for user: {recommendations_event.user_id}")
                    recommendations_event.state = State.in_progress
                    new_reccs, error = await self.recommendations.process_recommendations(user_id=recommendations_event.user_id)
                    if error:
                        print(
                            f"Error {error} calculating reccs for user: {recommendations_event.user_id}")
                        recommendations_event.state = State.fail
                    else:
                        print(
                            f"Successfully calculated Recommendations for user: {recommendations_event.user_id}")
                        recommendations_event.state = State.ok
                        recommendations_event.reccomendations = new_reccs

                    print(
                        f"Returning RecommendationsEvent for {recommendations_event.user_id}")
                    exception = await self.rabbitmq_client.publish(message=recommendations_event,
                                                                   routing_key=recommendations_event.result_routing_key)
                    if exception:
                        print(
                            f"Error {exception} when attempting to send recommendations event back")
                    else:
                        print(f"Published RecommendationsEvent back to it's source")

            except Exception as error:
                print(
                    f"Failure seen attempting to consume RecommendationEvents: {error}. Sleeping for 30 seconds")
                print(traceback.format_exc())
                await asyncio.sleep(30)


def main():
    app = AsyncRMQ()
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(app.consume_reccs_events()))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
