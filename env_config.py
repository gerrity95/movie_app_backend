from dotenv import load_dotenv
import os


class Config:
    """
    Config class for reading in env attributes
    """

    def __init__(self) -> None:

        load_dotenv()
        self.NODE_ENV = os.getenv('NODE_ENV')
        self.MONGO_HOSTNAME = os.getenv('MONGO_HOSTNAME')
        self.MONGO_PORT = os.getenv('MONGO_PORT')
        self.MONGO_USERNAME = os.getenv('MONGO_USERNAME')
        self.MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
        self.MONGO_DB = os.getenv('MONGO_DB')

        self.RMQ_HOST = os.getenv('RMQ_HOST')
        self.RMQ_PORT = os.getenv('RMQ_PORT')
        self.RMQ_USER = os.getenv('RMQ_USER')
        self.RMQ_PASSWORD = os.getenv('RMQ_PASSWORD')

        self.TMDB_API = os.getenv('TMDB_API')
        self.TMDB_READ_TOKEN = os.getenv('TMDB_READ_TOKEN')
        self.VALID_CORS = os.getenv('VALID_CORS')

        if self.NODE_ENV == 'tv':
            self.load_tv_configs()
        else:
            self.load_movie_configs()

    def load_tv_configs(self):
        self.ROUTING_KEY = 'television_recommendations'
        self.RECOMMENDATIONS_COLLECTION = 'recommended_televisions'
        self.RATED_COLLECTION = 'television_rateds'
        self.ID_KEY = 'tv_id'
        self.INFO_KEY = 'tv_info'

    def load_movie_configs(self):
        self.ROUTING_KEY = 'movie_recommendations'
        self.RECOMMENDATIONS_COLLECTION = 'recommended_movies'
        self.RATED_COLLECTION = 'rated_movies'
        self.ID_KEY = 'movie_id'
        self.INFO_KEY = 'movie_info'
