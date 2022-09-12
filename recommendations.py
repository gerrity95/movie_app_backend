import asyncio
from typing import Optional

from base.mongoclient import MongoClient
from base.tmdbclient import TmdbClient
from base.rabbitmq_client import RabbitMqClient
from base.recc_calculator import ReccCalculator
import json
import datetime
from base.recommendations_helper import RecommendationException, JSONEncoder, RecommendationsHelper
from base.events import RecommendationsEvent, State


class Recommendations:

    def __init__(self) -> None:
        self.mongo_client = MongoClient()
        self.tmdb_client = TmdbClient()
        self.rabbitmq_client = RabbitMqClient()
        self.recc_calculator = ReccCalculator()
        self.recc_helper = RecommendationsHelper()

    async def calculate_reccs(self, user_id: str):
        """
        """
        calc_start = datetime.datetime.now()
        # Check for existing recommendations
        stored_reccs, error = await self.recc_helper.query_mongo_for_user(user_id, 'recommended_movies')
        if error:
            print(f"Error {error} attempting to get recommended movies")
            return None, RecommendationException
        if stored_reccs:
            try:
                # TODO CHECK FOR STATE HERE IF ITS IN PROGRESS
                """
                EDGE CASE, user rates movie, goes to in progress, rates another movie before first movie finishes 
                processing. Do we generate them again?
                """
                print("Checking if we are currently updating the recommendations for user: " + user_id)
                if stored_reccs[0]['state'] == 'in_progress':
                    return await self.monitor_in_progress(user_id)

                encoded_reccs = JSONEncoder().encode(stored_reccs[0])
                encoded_reccs = json.loads(encoded_reccs)
                # Check against rated movies to see if we need to update the recommendations
                print("Comparing recommendations against existing ratings... ")
                need_new_reccs, error = await self.compare_reccs_with_rated(user_id=user_id,
                                                                            encoded_reccs=encoded_reccs)
                if error:
                    print(f"Error {error} seen attempting to compare recommendations with rated movies ")
                    return None, RecommendationException
            except Exception as e:
                print(f"Error {e} seen attempting to compare recommendations with rated movies ")
                return None, RecommendationException
        else:
            print("No recommendations have been generated. Sending request to RMQ to populate.... ")
            recommendations_event: RecommendationsEvent = await self.make_recommendation_request(user_id=user_id,
                                                                                                 is_new=True)
            calc_finish = datetime.datetime.now()
            print(f"Calculation Duration: {(calc_start - calc_finish).total_seconds()}")
            if recommendations_event.state != State.ok:
                print(f"Unable to calculate new Recommendations and there is no existing ones. Returning Exception....")
                return None, RecommendationException
            else:
                return recommendations_event.reccomendations, None

        if need_new_reccs:
            print(f"Recommendations have expired for user {user_id}. Sending request to RMQ to update... ")
            recommendations_event: RecommendationsEvent = \
                await self.make_recommendation_request(user_id=user_id,
                                                       is_new=False,
                                                       existing_reccs_id=stored_reccs[0]['_id'])
            calc_finish = datetime.datetime.now()
            print(f"Calculation Duration: {(calc_finish - calc_start).total_seconds()} seconds")
            if recommendations_event.state != State.ok:
                print(f"Unable to calculate new Recommendations. Returning existing ones....")
                return encoded_reccs['recommendations'], None
            else:
                return recommendations_event.reccomendations, None
        else:
            print(f"Recommendations are up to date for user {user_id}. Will not attempt to update ")
            return encoded_reccs['recommendations'], None

    async def monitor_in_progress(self, user_id) -> [Optional[dict], Optional[Exception]]:
        """
        Function to process logic if there are currently recommendations being generated
        """
        counter = 0
        while counter < 5:
            print("Currently in the process of updating the recommendations. Will retry in 5 seconds to "
                  "check if complete... ")
            await asyncio.sleep(5)
            stored_reccs, error = await self.recc_helper.query_mongo_for_user(user_id, 'recommended_movies')
            if error:
                print(f"Error {error} attempting to get reccommended movies ")
                return None, RecommendationException

            if stored_reccs[0]['state'] == 'in_progress':
                counter += 1
            else:
                # No need to generate them again so can just return. Want to wait until process is complete.
                print("Recommendations have been updated as part of another process. Returning. ")
                return stored_reccs[0]['recommendations'], None

    async def compare_reccs_with_rated(self, user_id, encoded_reccs: dict):
        """
        Function to compare the stored reccs with the most recent rated movie to see if the reccs need to be updated
        Will return True if we need to update the reccomendations
        """
        print(f"Recommendations stored for user {user_id}, checking to see if they're up to date.")
        reccs_updated = datetime.datetime.fromisoformat(encoded_reccs['updatedAt'])

        # Getting rated movies
        recent_movie, error = await self.recc_helper.most_recent_rated_movie(user_id)
        if error:
            print(f"Error {error} attempting to get rated movies")
            return None, RecommendationException
        encoded_recent = JSONEncoder().encode(recent_movie[0])
        encoded_recent = json.loads(encoded_recent)
        recent_updated = datetime.datetime.fromisoformat(encoded_recent['updatedAt'])
        if reccs_updated < recent_updated:
            return True, None
        else:
            return False, None

    async def make_recommendation_request(self, user_id: str, is_new: bool,
                                          existing_reccs_id: str = None) -> RecommendationsEvent:
        """
        Function that will make a request to RMQ and await a response to get our recommendations.
        """
        recommendation_event = RecommendationsEvent(user_id=user_id, is_new=is_new)
        if existing_reccs_id:
            recommendation_event.existing_reccs_id = existing_reccs_id

        print(f"Declaring return queue for {recommendation_event.user_id}")
        return_queue = await self.rabbitmq_client.declare_queue(routing_key=recommendation_event.result_routing_key,
                                                                durable=False,
                                                                auto_delete=True)

        print(f"Publishing RecommendationEvent for {recommendation_event.user_id}")
        error: Exception = await self.rabbitmq_client.publish(message=recommendation_event,
                                                              routing_key=recommendation_event.routing_key())

        if not error:
            print("Successfully published RecommendationsEvent")
            result, error = await self.rabbitmq_client.consume_first(
                routing_key=recommendation_event.result_routing_key,
                queue=return_queue, count=1)
            if result:
                recommendation_event: RecommendationsEvent = result[0]
                print(f"Succesfully got a result back from RMQ")
            else:
                print(f"Error {error} seen getting a result back from RMQ")

            print("Attempting to delete queue...")
            await self.rabbitmq_client.delete_queue(routing_key=recommendation_event.result_routing_key)
        else:
            print(f"Error: {error} seen attempting to publish RecommendationsEvent")
            await self.rabbitmq_client.delete_queue(routing_key=recommendation_event.result_routing_key)

        return recommendation_event
