from base.mongoclient import MongoClient
from base.tmdbclient import TmdbClient
from base.rabbitmq_client import RabbitMqClient
from typing import Optional, Tuple
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
        print(f"Getting data for all media.")
        
        result, error = await self.tmdb_client.get_media_information(media_ids=media_ids)    
        
        if error:
            print(f"Error {error} seen attempting to get media information for watchlist")
            return None, Exception
        
        print(f"Successfully got media information for all media in the watchlist")
        return result, None
    