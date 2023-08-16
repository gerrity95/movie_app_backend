from typing import Optional, Tuple
from base.mongoclient import MongoClient
from base.tmdbclient import TmdbClient
from base.rabbitmq_client import RabbitMqClient
from base.recommendations_helper import RecommendationException, RecommendationsHelper
from env_config import Config


class Watchlist:

    def __init__(self) -> None:
        self.config = Config()
        self.mongo_client = MongoClient()
        self.tmdb_client = TmdbClient()
        self.rabbitmq_client = RabbitMqClient()

    async def process_watchlist(self, media_list: list) -> Tuple[Optional[list], Optional[Exception]]:
        """
        Function to get all data for all movies in a given users watchlist
        """
        media_ids = []
        for media in media_list:
            media_ids.append(media[self.config.ID_KEY])

        print(f"List of media on watchlist: {media_ids}")
        print("Getting data for all media.")

        result, error = await self.tmdb_client.get_media_information(media_ids=media_ids)    

        if error:
            print(f"Error {error} seen attempting to get media information for watchlist")
            return None, Exception

        print("Successfully got media information for all media in the watchlist")
        return result, None


class Blocklist:
    
    def __init__(self) -> None:
        self.config = Config()
        self.mongo_client = MongoClient()
        self.recc_helper = RecommendationsHelper()
        self.rec_collection = self.mongo_client.recommended_collection()

    
    async def update_block_from_reccs(self, media_id: int, user_id: str, update_to: bool) -> Tuple[Optional[list], Optional[Exception]]:
        """
        Function to update a blocked movie's status from the recommendations
        """
        if self.config.NODE_ENV == 'tv':
            media_type = 'tv_id'
        else:
            media_type = 'movie_id'
        
        # Get existing recommendations
        try:
            print(f"Attempting to get existing recommendations for user {user_id}")
            stored_reccs = await self.rec_collection.find_one({"user_id": user_id})
            if not stored_reccs:
                print(f"No recommendations found for user: {user_id}")
                return None, f"No recommedations found for user {user_id}"
            
            print("Got Recommedations")
            print(media_id)
            has_updated = False
            recommendations = stored_reccs['recommendations']
            for index, media in enumerate(recommendations):
                if int(media_id) == int(media[media_type]):
                    print(f"Found entry in recommendations for {media_id}")
                    recommendations[index]['blocklist'] = update_to
                    print(recommendations[index])
                    has_updated = True
        except Exception as exception:
            print(f"Exception seen attempting to get recommendations for user {user_id}")
            print(exception)
            return None, exception

        if has_updated:       
            result = await self.rec_collection.update_one({'_id': stored_reccs['_id']}, {
                    '$set': {'recommendations': recommendations, 'state': 'complete'},
                    '$currentDate': {'updatedAt': True}})
            if result.modified_count > 0:
                print(f"Successfully updated recommendations for: {user_id}")
                return True, None
            else:
                return False, f"Unable to update recommendations for user {user_id}"
        else:
            print("Movie not part of the recommendations. Not updating")
            return True, None
        return result, None
