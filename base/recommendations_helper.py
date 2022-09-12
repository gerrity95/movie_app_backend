import json
import datetime
from base.mongoclient import MongoClient
from base.tmdbclient import TmdbClient
from collections import Counter
from base.recc_calculator import ReccCalculator
from bson import ObjectId


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (ObjectId, datetime.datetime)):
            return str(o)
        return json.JSONEncoder.default(self, o)


class RecommendationException(Exception):
    """
    class to handle exceptions in the Recommendations class
    """


class RecommendationsHelper:

    def __init__(self) -> None:
        self.mongo_client = MongoClient()
        self.tmdb_client = TmdbClient()
        self.recc_calculator = ReccCalculator()

    async def process_recommendations(self, user_id: str, is_new: bool, existing_reccs_id: str = None):
        """
        A function to gather and process all recommendations for the given user
        """
        try:
            rec_collection = self.mongo_client.recommended_collection()
            if is_new:
                print("First time generating recommendations. Creating empty recommendations object and "
                      "Appending to Mongo...")
                document = {'user_id': user_id, 'recommendations': {},
                            'createdAt': datetime.datetime.now(), 'updatedAt': datetime.datetime.now(),
                            'state': 'in_progress'}
                result = await rec_collection.insert_one(document)
            else:
                print("Attempting to update the recommendations. Setting state to in progress..")
                result = await rec_collection.update_one({'_id': existing_reccs_id}, {'$set': {'state': 'in_progress'},
                                                                                      '$currentDate': {
                                                                                          'updatedAt': True}})

            print(result)
            reccs_data, error = await self.gather_reccs_data(user_id)
            if error:
                print("Error attempting to get rated movies")
                return None, RecommendationException("Error attempting to gather recommendation data")

            print("Attempting to process recommendation data...")
            sorted_reccomendations = self.recc_calculator.do_calculate(tmdb_data=json.loads(reccs_data))
            print("Updating recommendations in Mongo...")
            if not existing_reccs_id:
                stored_reccs, error = await self.query_mongo_for_user(user_id, 'recommended_movies')
                existing_reccs_id = stored_reccs[0]['_id']

            result = await rec_collection.update_one({'_id': existing_reccs_id}, {
                '$set': {'recommendations': sorted_reccomendations, 'state': 'complete'},
                '$currentDate': {'updatedAt': True}})
            print(result)
            return sorted_reccomendations, None
        except Exception as err:
            print(f"Error {err} seen when attempting to calculate reccommendations")
            await rec_collection.update_one({'_id': existing_reccs_id},
                                            {'$set': {'state': 'failed'}, '$currentDate': {'updatedAt': True}})
            return None, Exception(str(err))

    async def gather_reccs_data(self, user_id: str):
        print("Attempting to gather all recommendation data...")
        rated_movies, error = await self.query_mongo_for_user(user_id, 'rated_movies')
        if error:
            print("Error attempting to get rated movies")
            return None, RecommendationException
        keywords = []
        for item in rated_movies:
            keywords.append(item['keywords'])
        directors, genres, keywords = self.extract_details_for_discover(rated_movies)

        discover_directors = []
        disc_direc, error = await self.tmdb_client.make_parallel_discover_request(type='director',
                                                                                  unique_id_list=directors)
        if error:
            print("Error attempting to get query discover")
            return None, RecommendationException
        for item in disc_direc:
            discover_directors.extend(item['results'])

        discover_genres = []
        disc_genre, error = await self.tmdb_client.make_parallel_discover_request(type='genre', unique_id_list=genres)
        if error:
            print("Error attempting to get query discover")
            return None, RecommendationException
        for item in disc_genre:
            discover_genres.extend(item['results'])

        discover_keywords = []
        disc_keywords, error = await self.tmdb_client.make_parallel_discover_request(type='keywords',
                                                                                     unique_id_list=keywords)
        if error:
            print("Error attempting to get query discover")
            return None, RecommendationException
        for item in disc_keywords:
            discover_keywords.extend(item['results'])

        top_movies = self.get_top_rated_movies(rated_movies)
        similar_movies, error = await self.tmdb_client.make_parallel_movie_request(path='similar', movies=top_movies)
        if error:
            print("Error attempting to get similar movies")
            return None, RecommendationException

        similar_movie_collection = []
        for item in similar_movies:
            similar_movie_collection.extend(item['results'])

        recommended_movies, error = await self.tmdb_client.make_parallel_movie_request(path='recommendations',
                                                                                       movies=top_movies)
        if error:
            print("Error attempting to get recommended movies")
            return None, RecommendationException
        recommended_movie_collection = []
        for item in recommended_movies:
            recommended_movie_collection.extend(item['results'])

        full_response = {'discover_directors': discover_directors, 'discover_genres': discover_genres,
                         'discover_keywords': discover_keywords,
                         'similar_movies': similar_movie_collection,
                         'recommeded_movies': recommended_movie_collection,
                         'rated_movies': rated_movies,
                         'directors': directors,
                         'genres': genres}

        return JSONEncoder().encode(full_response), error

    @staticmethod
    def get_top_rated_movies(rated_movie: dict):
        """
        Get the top rated movies for the given user
        """
        movie_list = []
        for movie in rated_movie:
            if movie['rating'] > 6:
                simple_movie = {'movie_id': movie['movie_id'], 'rating': movie['rating']}
                movie_list.append(simple_movie)

        ordered_movies = sorted(movie_list, key=lambda i: i['rating'], reverse=True)

        # Returns 20 highest rated movies
        return ordered_movies[0:19]

    @staticmethod
    def extract_details_for_discover(rated_movie: dict):
        """
        Function to get relevant details from my existing rated movies for discover query
        """

        genres = []
        directors = []
        keywords = []
        for item in rated_movie:
            genres.append(item['genres'])
            directors.append(item['director'])
            keywords.append(item['keywords'])

        direc_counts = Counter(directors)
        most_common_direcs = direc_counts.most_common(6)

        list_of_g_ids = []
        for genre in genres:
            genre_string = ''
            for item in genre:
                genre_string += str(item['id'])
                if item != genre[-1]:
                    genre_string += ','
            list_of_g_ids.append(genre_string)

        genre_counts = Counter(list_of_g_ids)

        list_of_keyword_sets = []
        for keyword_set in keywords:
            for keyword in keyword_set:
                list_of_keyword_sets.append(keyword['id'])

        keyword_counts = Counter(list_of_keyword_sets)

        most_common_genres = genre_counts.most_common(6)
        most_common_keywords = keyword_counts.most_common(6)

        return most_common_direcs, most_common_genres, most_common_keywords

    async def query_mongo_for_user(self, user_id, collection):
        """
        Function to get info from a given collection from a given user
        """
        try:
            query = self.movies_query_build(user_id)
            rated_movies, error = await self.mongo_client.make_request(collection=collection, query=query)
        except Exception as err:
            print(f"Error: {err} when attempting to get query Mongo for collection: {collection}")
            return None, err

        return rated_movies, error

    async def most_recent_rated_movie(self, user_id):
        """
        Function to query mongo to get the most recent rated movie
        """
        try:
            query = self.recent_movie_query(user_id)
            rated_movies, error = await self.mongo_client.make_request(collection='rated_movies', query=query)
        except Exception as err:
            print(f"Error: {err} when attempting to get query Mongo for collection: rated_mvoies")
            return None, err

        return rated_movies, error

    @staticmethod
    def recent_movie_query(user_id) -> list:
        """
        Function to build out the query to get most recent rated movie
        """

        pipeline = [
            {
                "$match": {
                    "user_id": user_id
                }
            },
            {
                "$sort": {
                    "updatedAt": -1
                }
            }
        ]

        return pipeline

    @staticmethod
    def movies_query_build(user_id) -> list:
        """
        Function to build out the query to get rated movies
        """

        pipeline = [
            {
                "$match": {
                    "user_id": user_id
                }
            }
        ]

        return pipeline
