import requests
import json
from env_config import Config
import asyncio
import aiohttp


class TmdbClient:
    """
    A generic tmdb client
    """

    def __init__(self) -> None:
        self.config = Config()
        self.api_key = self.config.TMDB_API
        self.read_token = self.config.TMDB_READ_TOKEN
        self.api_endpoint = 'https://api.themoviedb.org/3/'

    async def ping(self) -> bool:
        try:
            headers = {
                'Authorization': f"Bearer {self.read_token}"
            }
            requests.get(url=f"{self.api_endpoint}/account", headers=headers)
            return "True", 200
        except Exception as error:
            print(f"Error talking to TMDB: {error}")
            return "Internal Server Error", 500

    async def make_media_request(self, path: str, media_id: int):
        """
        Function to make request against TMDB API
        """
        print(f"Making request against media endpoint for media: {media_id}")
        try:
            headers = {
                'Authorization': f"Bearer {self.read_token}"
            }
            print(f"Url is: {self.api_endpoint}/{self.config.NODE_ENV}/{media_id}/{path}")
            result = requests.get(url=f"{self.api_endpoint}/{self.config.NODE_ENV}/{media_id}/{path}",
                                  headers=headers)
        except Exception as error:
            print(f"Error attempting to make request against tmdb: {error}")
            return None, error

        if result.status_code == 200:
            print("Successfully got a response from generic media endpoint...")
            try:
                return result.json(), None
            except json.decoder.JSONDecodeError as err:
                print("Error with the response returned TMDB. Cleaning up")
                return None, err
        else:
            print(f"Unexpected response from TMDB. Status: {result.status_code}, content: {result.content}")
            return None, Exception

    async def get(self, url, session):
        headers = {
            'Authorization': f"Bearer {self.read_token}"
        }
        try:
            async with session.get(url=url, headers=headers) as response:
                resp = await response.read()
                print("Successfully got url {} with resp of length {}.".format(url, len(resp)))
                return resp
        except Exception as e:
            print("Unable to get url {} due to {}.".format(url, e.__class__))

    async def make_parallel_media_request(self, medias: list, path):
        urls = []
        try:
            for media in medias:
                urls.append(f"{self.api_endpoint}/{self.config.NODE_ENV}/{media[self.config.ID_KEY]}/{path}")

            async with aiohttp.ClientSession() as session:
                ret = await asyncio.gather(*[self.get(url, session) for url in urls])
            print("Finalized all. Return is a list of len {} outputs.".format(len(ret)))

            # Convert items from BYTES to JSON
            completed = []
            for item in ret:
                completed.append(json.loads(item))

            return completed, None

        except Exception as e:
            print(f"Error {e} attempting to talk to TMDB.")
            return None, Exception

    async def make_discover_request(self, type: str, unique_id: str):
        """
        Function to make request against TMDB discover API
        """
        print(f"Making Request against discover endpoint for type: {type} with unique_id: {unique_id}")
        try:
            params = {
                'sort_by': 'vote_average.desc',
                'vote_count.gte': 1000,
                'with_original_language': 'en',
                'page': '1',

            }
            if type == 'director':
                params['with_crew'] = unique_id
            elif type == 'genre':
                params['with_genres'] = unique_id
            else:
                params['with_keywords'] = unique_id

            headers = {
                'Authorization': f"Bearer {self.read_token}"
            }
            result = requests.get(url=f"{self.api_endpoint}/discover/{self.config.NODE_ENV}/",
                                  headers=headers, params=params)
        except Exception as error:
            print(f"Error attempting to make request against tmdb: {error}")
            return None, error

        if result.status_code == 200:
            print("Successfully got a response from discover endpoint...")
            try:
                return result.json(), None
            except json.decoder.JSONDecodeError as err:
                print("Error with the response returned TMDB. Cleaning up")
                return None, err
        else:
            print(f"Unexpected response from TMDB. Status: {result.status_code}, content: {result.content}")
            return None, Exception

    async def make_parallel_discover_request(self, unique_id_list: str, request_type: str):
        urls = []
        try:
            params = {
                'sort_by': 'vote_average.desc',
                'vote_count.gte': 1000,
                'with_original_language': 'en',
                'page': '1',

            }
            for unique_id in unique_id_list:
                if request_type == 'director':
                    params['with_crew'] = unique_id[0]
                elif request_type == 'genre':
                    params['with_genres'] = unique_id[0]
                else:
                    params['with_keywords'] = unique_id[0]

                param_string = ''
                for key, value in params.items():
                    param_string += f"&{key}={value}"

                urls.append(f"{self.api_endpoint}discover/{self.config.NODE_ENV}?{param_string}")

            async with aiohttp.ClientSession() as session:
                ret = await asyncio.gather(*[self.get(url, session) for url in urls])
            print(f"Finalized all. Return is a list of len {len(ret)} outputs.")

            # Convert items from BYTES to JSON
            completed = []
            for item in ret:
                completed.append(json.loads(item))

            # Append the director ID and keywords to the results so they can be used in the calculation algo.
            if request_type == 'director':
                for index, discover_result in enumerate(completed):
                    for media in discover_result['results']:
                        media['director'] = unique_id_list[index][0]
                    
            if request_type == 'keywords':
                for index, discover_result in enumerate(completed):
                    for media in discover_result['results']:
                        media['keywords'] = unique_id_list[index][0]

            return completed, None

        except Exception as err:
            print(f"Error {err} attempting to talk to TMDB.")
            return None, Exception

    async def get_media_information(self, media_ids: list):
        """
        Function that will get movie information for a list of movie IDs
        """
        urls = []
        try:
            for id in media_ids:
                urls.append(f"{self.api_endpoint}{self.config.NODE_ENV}/{id}")

            async with aiohttp.ClientSession() as session:
                ret = await asyncio.gather(*[self.get(url, session) for url in urls])
            print("Finalized all. Return is a list of len {} outputs.".format(len(ret)))

            # Convert items from BYTES to JSON
            completed = []
            for item in ret:
                completed.append(json.loads(item))

            return completed, None

        except Exception as e:
            print(f"Error {e} attempting to talk to TMDB.")
            return None, Exception
