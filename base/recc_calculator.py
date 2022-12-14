from collections import Counter
from env_config import Config


class ReccCalculator:
    
    def __init__(self) -> None:
        self.config = Config()
    
    def do_calculate(self, tmdb_data: dict) -> list:
        
        # TODO SOME ERROR HANDLING
        
        discover_directors = tmdb_data['discover_directors']
        discover_keywords = tmdb_data['discover_keywords']
        discover_genres = tmdb_data['discover_genres']
        similar_movies = tmdb_data['similar_movies']
        recommeded_movies = tmdb_data['recommeded_movies']
        rated_movies = tmdb_data['rated_movies']
        directors = tmdb_data['directors']
        genres = tmdb_data['genres']

        discovered_data = []
        discovered_data.extend(discover_genres)
        discovered_data.extend(discover_keywords)
        discovered_data.extend(discover_directors)
        discovered_data.extend(similar_movies)
        discovered_data.extend(recommeded_movies)
        
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
        media_weights = self.assign_voting_weight(media_weights=media_weights, discovered_data=discovered_data)
    
        # Format results
        formatted_results = self.format_results(media_weights=media_weights, discovered_data=discovered_data)
        
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

    
    def format_results(self, media_weights, discovered_data) -> list:
        formatted_results = []
        for key, value in media_weights.items():
            formatted_media = {self.config.ID_KEY: key, 'weight': value}
            for media in discovered_data:
                if key == media['id']:
                    formatted_media[self.config.INFO_KEY] = media
                    break
            formatted_results.append(formatted_media)

        formatted_results = sorted(formatted_results, key=lambda k: k['weight'], reverse=True)

        return formatted_results
