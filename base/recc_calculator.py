"""
Algorithim for calculating the recommendations for a user

What is in tmdb_data? See recommendations_helper.gather_recc_data

    We get all the movies a user has already rated

    We extract the directors, genres and keywords from the movies that have been rated

    We then make a 'discover' request against TMDB using the list of directors, genres, keywords we got above

    ---

    We extract the 'top rated media' (only rated > 6) from the list existing rated movies and we add the top 20 movies into a list

    We then query for the 'similar movies' and the 'recommended movies' from this top rated media list

    All this above data makes up the tmdb_data dictionary + the list of movies the user has already rated

We extract the movies the list of movies the user has already rated and then combine all the rest of the tmdb_data into a list called discovery_data

We remove the movies that the user has already rated from the discovery_data

We append the movie Ids into a new list called movie_weights

We run a counter on movie_weights to give us the number of occurrences for a movie in the discovery data

We then remove all duplicate movies from the discovery data - The counter above has already given us the value of how many times these movies appeared

We then increase weights based on the genres:
    We get all genres (g1) that are extracted from movies that are already rated
    We loop through each movie in our discovery data:
        for each of the genres in g1 -> If it appears in discovery_data for a given movie -> we add +1 to the weight of the movie in the movie_weight dict

We then assign increase weights based on the movie rating in TMDB itself:
    We append the movie rating to the weight for that given movie, e.g. if a movie is rated 84% we will append 8.4 to the overall recommendation weight for that movie



"""

from collections import Counter
from env_config import Config


class ReccCalculator:

    def __init__(self) -> None:
        self.config = Config()

    def do_calculate(self, tmdb_data: dict) -> list:
        '''
            Function that generates the recommendations for a user
            # TODO SOME ERROR HANDLING
        '''

        discover_directors = tmdb_data['discover_directors']
        discover_networks = tmdb_data['discover_networks']
        discover_keywords = tmdb_data['discover_keywords']
        discover_genres = tmdb_data['discover_genres']
        similar_movies = tmdb_data['similar_movies']
        recommeded_movies = tmdb_data['recommeded_movies']
        rated_movies = tmdb_data['rated_movies']
        directors = tmdb_data['directors']
        genres = tmdb_data['genres']
        keywords = tmdb_data['keywords']
        networks = tmdb_data['networks']

        discovered_data = []
        discovered_data.extend(discover_genres)
        discovered_data.extend(discover_keywords)
        discovered_data.extend(discover_directors)
        discovered_data.extend(discover_networks)
        discovered_data.extend(similar_movies)
        discovered_data.extend(recommeded_movies)

        print("NETWORKS")
        print(discover_networks)

        print("DIRECTORS")
        print(discover_directors)

        # REMOVE ALL RATED MOVIES
        existing_ids = []
        for item in rated_movies:
            existing_ids.append(item[self.config.ID_KEY])

        discovered_data = self.delete_existing(discovered_data, existing_ids)

        media_weights = []

        for media in discovered_data:
            media_weights.append(media['id'])

        media_weights = Counter(media_weights)

        # Remove duplicates of movies from discovery list
        for key, value in media_weights.items():
            occurrences = []
            for index, item in enumerate(discovered_data):
                if value > 1 and item['id'] == key:
                    occurrences.append(index)
            if occurrences:
                sorted_occurence = sorted(occurrences, reverse=True)
                for i in sorted_occurence:
                    if i == sorted_occurence[-1]:
                        pass
                    else:
                        del discovered_data[i]

        # Assign weight from genres
        media_weights = self.assign_genre_weight(media_weights=media_weights, genres=genres,
                                                 discovered_data=discovered_data)

        # Assign weight from ratings
        media_weights = self.assign_voting_weight(
            media_weights=media_weights, discovered_data=discovered_data)

        # Assign weight from directors
        media_weights = self.assign_director_weight(
            media_weights=media_weights, discovered_data=discovered_data, directors=directors)

        # Assign weight from networks for tv only
        if self.config.NODE_ENV == "tv":
            media_weights = self.assign_networks_weight(
                media_weights=media_weights, discovered_data=discovered_data, networks=networks)

        # Assign weight from keywords
        media_weights = self.assign_keyword_weight(
            media_weights=media_weights, discovered_data=discovered_data, keywords=keywords)

        # Format results
        formatted_results = self.format_results(
            media_weights=media_weights, discovered_data=discovered_data)

        return formatted_results

    @staticmethod
    def delete_existing(rec_list, existing_id_list) -> list:
        existing_occurences = []
        for index, item in enumerate(rec_list):
            if item['id'] in existing_id_list:
                existing_occurences.append(index)
        for i in sorted(existing_occurences, reverse=True):
            del rec_list[i]
        return rec_list

    @staticmethod
    def assign_genre_weight(media_weights, genres, discovered_data):
        genre_id_list = []
        for genre in genres:
            g = genre[0].split(',')
            genre_id_list.extend(g)
        genre_id_list = set(genre_id_list)
        for movie in discovered_data:
            for genre in movie['genre_ids']:
                if str(genre) in genre_id_list:
                    for key, value in media_weights.items():
                        if movie['id'] == key:
                            media_weights[key] += 1

        return media_weights

    @staticmethod
    def assign_voting_weight(media_weights, discovered_data):
        for movie in discovered_data:
            rating = movie['vote_average']
            for key, value in media_weights.items():
                if movie['id'] == key:
                    media_weights[key] += round(rating, 3)

        return media_weights

    @staticmethod
    def assign_director_weight(media_weights, discovered_data, directors):
        '''
        Function that will identify the users most frequently rated director and append this to the movie weight. 
        '''
        for movie in discovered_data:
            # Not all movies will have the director populated
            if 'director' in movie:
                dir_id = movie['director']
                movie_id = movie['id']
                for dirs in directors:
                    if dir_id == dirs[0]:
                        media_weights[movie_id] += dirs[1]

        return media_weights

    @staticmethod
    def assign_networks_weight(media_weights, discovered_data, networks):
        '''
        Function that will identify the users most frequently rated networks and append this to the movie weight. 
        '''
        print("NETWORK WEIGHT")
        for media in discovered_data:
            # Not all movies will have the director populated
            if 'networks' in media:
                print(f'BAH: {media}')
                print(f"Boom: {type(media['networks'])}")
                network = media['networks']
                print(f"network: {network}")
                print(f"media_id: {media['id']}")
                media_id = media['id']
                for netw in networks:
                    print(f"netw: {netw}")
                    if network == netw[0]:
                        media_weights[media_id] += netw[1]

        return media_weights

    @staticmethod
    def assign_keyword_weight(media_weights, discovered_data, keywords):
        '''
        Function that will identify the users most frequently rated keywords and append this to the movie weight. 
        '''
        for movie in discovered_data:
            # Not all movies will have the director populated
            if 'keywords' in movie:
                mov_keyword = movie['keywords']
                movie_id = movie['id']
                for key in keywords:
                    if mov_keyword == key[0]:
                        media_weights[movie_id] += key[1]

        return media_weights

    def format_results(self, media_weights, discovered_data) -> list:
        '''
        Function to parse the recommendations and populate them with relevant data
        '''

        sorted_weights = media_weights.most_common()
        baseline = 0
        updated_weights = []
        formatted_results = []

        for index, value in enumerate(sorted_weights):
            if index == 0:
                baseline = value[1]
            updated_weights.append(
                {value[0]: round(100 / baseline * value[1])})

        for item in updated_weights:
            media_key = list(item.keys())[0]
            media_weight = list(item.values())[0]
            formatted_media = {
                self.config.ID_KEY: media_key, 'weight': media_weight}
            for media in discovered_data:
                if media_key == media['id']:
                    formatted_media[self.config.INFO_KEY] = media
                    break
            formatted_results.append(formatted_media)

        formatted_results = sorted(
            formatted_results, key=lambda k: k['weight'], reverse=True)

        return formatted_results
