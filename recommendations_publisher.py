import asyncio
from base.events import RecommendationsEvent
from base.rabbitmq_client import RabbitMqClient


class RecommendationPublisher:

    def __init__(self, rabbitmq_client: RabbitMqClient) -> None:
        self.rabbitmq_client = rabbitmq_client

    async def main(self):
        
        recommendation_event = RecommendationsEvent()
        
        return_queue = await self.rabbitmq_client.declare_queue(routing_key=recommendation_event.result_routing_key,
                                                                durable=False,
                                                                auto_delete=True)
        
        error: Exception = await self.rabbitmq_client.publish(message=recommendation_event, 
                                                              routing_key=recommendation_event.routing_key())
        
        if not error:
            print("Successfully published Recommendations Event")
            result, error = await self.rabbitmq_client.consume_first(routing_key=recommendation_event.result_routing_key,
                                                                     queue=return_queue, count=1)
            if result:
                recommendation_event: RecommendationsEvent = result[0]
                print(f"Succesfully got a result back from RMQ")
                print(recommendation_event)
                print(recommendation_event.test_attribute)
            else:
                print(f"Error {error} seen getting a result back from RMQ")
            
            print("Attempting to delete queue...")
            await asyncio.sleep(4)
            await self.rabbitmq_client.delete_queue(routing_key=recommendation_event.result_routing_key)
        else:
            print(f"Error: {error} seen attempting to publish RecommendationsEvent")
            await self.rabbitmq_client.delete_queue(routing_key=recommendation_event.result_routing_key)