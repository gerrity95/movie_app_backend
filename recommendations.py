import asyncio
from typing import Optional

from env_config import Config
from base.mongoclient import MongoClient
from base.tmdbclient import TmdbClient
from base.rabbitmq_client import RabbitMqClient
import json
import datetime
from base.recommendations_helper import RecommendationException, JSONEncoder, RecommendationsHelper
from base.recc_calculator import ReccCalculator


class Recommendations:

    def __init__(self) -> None:
        self.config = Config()
        self.mongo_client = MongoClient()
        self.tmdb_client = TmdbClient()
        self.rabbitmq_client = RabbitMqClient()
        self.recc_helper = RecommendationsHelper()
        self.recc_calculator = ReccCalculator()
        self.rec_collection = self.mongo_client.recommended_collection()

    async def process_recommendations(self, user_id: str):
        """
        Logic to identify we need to process a new recommendations request
        """
        # Check for existing recommendations
        stored_reccs, error = await self.recc_helper.query_mongo_for_user(user_id, self.config.RECOMMENDATIONS_COLLECTION)
        if error:
            print(f"Error {error} attempting to get recommended media")
            return None, RecommendationException
        
        if stored_reccs:
            print('Found existing recommendations object. Will update existing.')
            # Check if we need to generate new recommendations
            need_new_reccs, error = await self.handle_stored_reccs(user_id=user_id, stored_reccs=stored_reccs)
            if error:
                return None, error
            
            if need_new_reccs:
                # Apply logic to generate new recommendations
                recommendations, err = await self.generate_new_recommendations(user_id=user_id, is_new=False, existing_reccs=stored_reccs[0]['_id'])
            else:
                # Return existing recommendations
                encoded_reccs = JSONEncoder().encode(stored_reccs[0])
                encoded_reccs = json.loads(encoded_reccs)
                return encoded_reccs, None
        else:
            # Apply logic to generate new recommendations
            print('No existing recommendations found. Generating new ones')
            recommendations, err = await self.generate_new_recommendations(user_id=user_id, is_new=True)
        
        return recommendations, err

    async def generate_new_recommendations(self, user_id: str, is_new: bool, existing_reccs=None):
        """
        Handle the logic to generate the new recommendations
        """
        try:
            print('Setting the recommendations to in progress in our database')
            updated_doc = await self.recc_helper.set_in_progress(user_id=user_id, is_new=is_new, existing_reccs=existing_reccs)
            
            print('Attempting to gather rated data from the database')
            recc_data, error = await self.recc_helper.gather_reccs_data(user_id=user_id)
            if error:
                print(f"Error {error} attempting to gather rated data")
                return None, Exception

            print("Attempting to process recommendation data...")
            sorted_reccomendations = self.recc_calculator.do_calculate(tmdb_data=json.loads(recc_data))

            print("Updating recommendations in Mongo...")
            if not existing_reccs:
                print('Newly created entry')
                print(updated_doc.inserted_id)
                # For newly created entries
                existing_reccs = updated_doc.inserted_id 
                
            print(existing_reccs)
            
            result = await self.rec_collection.update_one({'_id': existing_reccs}, {
                '$set': {'recommendations': sorted_reccomendations, 'state': 'complete'},
                '$currentDate': {'updatedAt': True}})
            print(result)
            return sorted_reccomendations, None
        except Exception as err:
            print(f"Error {err} seen when attempting to calculate reccommendations")
            await self.rec_collection.update_one({'_id': existing_reccs},
                                                 {'$set': {'state': 'failed'}, 
                                                  '$currentDate': {'updatedAt': True}})
            return None, Exception(str(err))
        

        
    
    async def handle_stored_reccs(self, user_id, stored_reccs):
        """
        Function to handle processing of existing reccs and logic around if we need new ones or if we are currently generating reccs.
        
        EDGE CASE, user rates movie, goes to in progress, rates another movie before first movie finishes 
        processing. Do we generate them again?
        """
        try:
            print("Checking if we are currently updating the recommendations for user: " + user_id)
            if stored_reccs[0]['state'] == 'in_progress':
                return await self.recc_helper.monitor_in_progress(user_id)

            # Check against rated movies to see if we need to update the recommendations
            encoded_reccs = JSONEncoder().encode(stored_reccs[0])
            encoded_reccs = json.loads(encoded_reccs)
            print("Comparing recommendations against existing ratings... ")
            need_new_reccs, error = await self.compare_reccs_with_rated(user_id=user_id, encoded_reccs=encoded_reccs)
            if error:
                print(f"Error {error} seen attempting to compare recommendations with rated media")
                return None, RecommendationException
            
            print(f"User needs new recommendations: {need_new_reccs}")
            return need_new_reccs, None
        except Exception as e:
            print(f"Error {e} seen attempting to compare recommendations with rated media")
            return None, RecommendationException

    async def compare_reccs_with_rated(self, user_id, encoded_reccs: dict):
        """
        Function to compare the stored reccs with the most recent rated movie to see if the reccs need to be updated
        Will return True if we need to update the reccomendations
        """
        print(f"Recommendations stored for user {user_id}, checking to see if they're up to date.")
        reccs_updated = datetime.datetime.fromisoformat(encoded_reccs['updatedAt'])

        # Getting rated media
        recent_media, error = await self.recc_helper.most_recent_rated_media(user_id)
        if error:
            print(f"Error {error} attempting to get rated media")
            return None, RecommendationException
        encoded_recent = JSONEncoder().encode(recent_media[0])
        encoded_recent = json.loads(encoded_recent)
        recent_updated = datetime.datetime.fromisoformat(encoded_recent['updatedAt'])
        print(f"RECCS UPDATED: {reccs_updated}")
        print(f"RECENT UPLOADED: {recent_updated}")
        if reccs_updated < recent_updated:
            return True, None
        else:
            return False, None