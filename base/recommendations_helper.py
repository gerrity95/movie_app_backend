import json
import datetime
from base.mongoclient import MongoClient
from base.tmdbclient import TmdbClient
from collections import Counter
from base.recc_calculator import ReccCalculator
from env_config import Config
import asyncio
from typing import Optional


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
        self.config = Config()
        self.mongo_client = MongoClient()
        self.tmdb_client = TmdbClient()
        self.recc_calculator = ReccCalculator()

    async def monitor_in_progress(self, user_id) -> Optional[dict]:
        """
        Function to process logic if there are currently recommendations being generated
        """
        counter = 0
        while counter < 5:
            print("Currently in the process of updating the recommendations. Will retry in 5 seconds to "
                    "check if complete... ")
            await asyncio.sleep(5)
            stored_reccs, error = await self.recc_helper.query_mongo_for_user(user_id, self.config.RECOMMENDATIONS_COLLECTION)
            if error:
                print(f"Error {error} attempting to get reccommended media ")
                return None, RecommendationException

            if stored_reccs[0]['state'] == 'in_progress':
                counter += 1
            else:
                # No need to generate them again so can just return. Want to wait until process is complete.
                print("Recommendations have been updated as part of another process. Returning. ")
                return stored_reccs[0]['recommendations'], None
            
    async def set_in_progress(self, user_id: str, is_new: bool, existing_reccs=None):
        """
        Set the reccs object in the DB to in progress
        """
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
            result = await rec_collection.update_one({'_id': existing_reccs}, {'$set': {'state': 'in_progress'},
                                                                                        '$currentDate': {
                                                                                        'updatedAt': True}})
        return result


    async def gather_reccs_data(self, user_id: str):
        print("Attempting to gather all recommendation data...")
        rated_media, error = await self.query_mongo_for_user(user_id, self.config.RECOMMENDATIONS_COLLECTION)
        if error:
            print("Error attempting to get rated media")
            return None, RecommendationException
        keywords = []
        for item in rated_media:
            keywords.append(item['keywords'])
        directors, genres, keywords = self.extract_details_for_discover(rated_media)

        discover_directors = []
        disc_direc, error = await self.tmdb_client.make_parallel_discover_request(type='director',
                                                                                  unique_id_list=directors)
        if error:
            print("Error attempting to get query discover for directors")
            return None, RecommendationException
        for item in disc_direc:
            discover_directors.extend(item['results'])

        discover_genres = []
        disc_genre, error = await self.tmdb_client.make_parallel_discover_request(type='genre', unique_id_list=genres)
        if error:
            print("Error attempting to get query discover for genres")
            return None, RecommendationException
        for item in disc_genre:
            discover_genres.extend(item['results'])

        discover_keywords = []
        disc_keywords, error = await self.tmdb_client.make_parallel_discover_request(type='keywords',
                                                                                     unique_id_list=keywords)
        if error:
            print("Error attempting to get query discover for keywords")
            return None, RecommendationException
        for item in disc_keywords:
            discover_keywords.extend(item['results'])

        top_media = self.get_top_rated_movies(rated_media)
        similar_media, error = await self.tmdb_client.make_parallel_media_request(path='similar', medias=top_media)
        if error:
            print("Error attempting to get similar movies")
            return None, RecommendationException

        similar_media_collection = []
        for item in similar_media:
            similar_media_collection.extend(item['results'])

        recommended_media, error = await self.tmdb_client.make_parallel_media_request(path='recommendations',
                                                                                      medias=top_media)
        if error:
            print("Error attempting to get recommended movies")
            return None, RecommendationException
        recommended_movie_collection = []
        for item in recommended_media:
            recommended_movie_collection.extend(item['results'])

        full_response = {'discover_directors': discover_directors, 'discover_genres': discover_genres,
                         'discover_keywords': discover_keywords,
                         'similar_movies': similar_media_collection,
                         'recommeded_movies': recommended_movie_collection,
                         'rated_movies': rated_media,
                         'directors': directors,
                         'genres': genres}

        return JSONEncoder().encode(full_response), error

    def get_top_rated_movies(self, rated_media: dict):
        """
        Get the top rated movies for the given user
        """
        media_list = []
        for media in rated_media:
            if media['rating'] > 6:
                simple_media = {self.config.ID_KEY: media[self.config.ID_KEY], 'rating': media['rating']}
                media_list.append(simple_media)

        ordered_media = sorted(media_list, key=lambda i: i['rating'], reverse=True)

        # Returns 20 highest rated movies
        return ordered_media[0:19]

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
            query = self.media_query_build(user_id)
            rated_movies, error = await self.mongo_client.make_request(collection=collection, query=query)
        except Exception as err:
            print(f"Error: {err} when attempting to get query Mongo for collection: {collection}")
            return None, err

        return rated_movies, error

    async def most_recent_rated_media(self, user_id):
        """
        Function to query mongo to get the most recent rated movie
        """
        try:
            query = self.recent_media_query(user_id)
            rated_movies, error = await self.mongo_client.make_request(collection='rated_movies', query=query)
        except Exception as err:
            print(f"Error: {err} when attempting to get query Mongo for collection: rated_mvoies")
            return None, err

        return rated_movies, error

    @staticmethod
    def recent_media_query(user_id) -> list:
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
    def media_query_build(user_id) -> list:
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
