import asyncio
import datetime
from base.events import RecommendationsEvent, State
from base.rabbitmq_client import RabbitMqClient
from base.recommendations_helper import RecommendationException


class RecommendationPublisher:

    def __init__(self) -> None:
        self.rabbitmq_client = RabbitMqClient()

    async def main(self, user_id):
        calc_start = datetime.datetime.now()
        recommendation_event = RecommendationsEvent()
        recommendation_event.user_id = user_id
        print(recommendation_event.deconstruct())
        return_queue, error = await self.rabbitmq_client.declare_queue(routing_key=recommendation_event.result_routing_key,
                                                                durable=False,
                                                                auto_delete=True)
    
        error: Exception = await self.rabbitmq_client.publish_new(message=recommendation_event.deconstruct(),
                                                              routing_key=recommendation_event.routing_key(),
                                                              correlation_id=recommendation_event.result_routing_key)
                
        if not error:
            print("Successfully published RecommendationsEvent. Will wait to consume result...")
            result, error = await self.rabbitmq_client.consume_first(routing_key=recommendation_event.result_routing_key,
                                                                     queue=return_queue, count=1)
            if result:
                recommendation_event: RecommendationsEvent = RecommendationsEvent.reconstruct(result)
                print(f"Succesfully got a result back from RMQ")
                calc_finish = datetime.datetime.now()
                print(f"Calculation Duration: {(calc_finish - calc_start).total_seconds()}")
                recommendation_event.duration = (calc_finish - calc_start).total_seconds()
            else:
                print(f"Error {error} seen getting a result back from RMQ")

            print("Attempting to delete queue...")
            await self.rabbitmq_client.delete_queue(routing_key=recommendation_event.result_routing_key)
        else:
            print(f"Error: {error} seen attempting to publish RecommendationsEvent")
            await self.rabbitmq_client.delete_queue(routing_key=recommendation_event.result_routing_key)

        return recommendation_event, None