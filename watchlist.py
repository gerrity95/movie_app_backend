from base.mongoclient import MongoClient
from base.tmdbclient import TmdbClient
from base.rabbitmq_client import RabbitMqClient
from typing import Optional, Tuple


class Watchlist:
    
    def __init__(self) -> None:
        self.mongo_client = MongoClient()
        self.tmdb_client = TmdbClient()
        self.rabbitmq_client = RabbitMqClient()
    
    async def process_watchlist(self, movie_list: list) -> Tuple[Optional[list], Optional[Exception]]:
        """
        Function to get all data for all movies in a given users watchlist
        """
        movie_ids = []
        for movie in movie_list:
            movie_ids.append(movie['movie_id'])
        
        print(f"List of movies on watchlist: {movie_ids}")
        print(f"Getting movie information for all movies.")
        
        result, error = await self.tmdb_client.get_movie_information(movie_ids=movie_ids)    
        
        if error:
            print(f"Error {error} seen attempting to get movie information for watchlist")
            return None, Exception
        
        print(f"Successfully got movie information for all movies in the watchlist")
        return result, None
    