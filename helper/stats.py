from typing import Dict, Any

def get_stats() -> Dict[str, Dict[str, Any]]:
    """
    Returns statistical data for tables with specified attributes and relationships.
    """
    stats = {'student': {'n_r': 49, 'b_r': 1, 'l_r': 162, 'f_r': 49, 'v_a_r': {'id': 49, 'name': 49, 'dept_name': 16, 'total_cred': 15}},
             'department': {'n_r': 19, 'b_r': 1, 'l_r': 128, 'f_r': 19, 'v_a_r': {'dept_name': 16, 'building': 12, 'budget': 13}},
                     "movies": {
            "n_r": 1000,               # Total number of movies (tuples)
            "b_r": 60,                 # Total number of storage blocks
            "l_r": 512,                # Size of a single movie record (in bytes)
            "f_r": 16,                 # Blocking factor (movies per block)
            "v_a_r": {                 # Distinct values for attributes
                "movie_id": 1000,      # Each movie has a unique ID
                "title": 980,          # Number of unique movie titles (some may repeat)
                "genre": 20,           # Number of distinct genres
                "age_rating": 4
            }
        },
        "reviews": {
            "n_r": 5000,               # Total number of reviews (tuples)
            "b_r": 100,                # Total number of storage blocks
            "l_r": 256,                # Size of a single review record (in bytes)
            "f_r": 50,                 # Blocking factor (reviews per block)
            "v_a_r": {                 # Distinct values for attributes
                "review_id": 5000,     # Each review has a unique ID
                "movie_id": 1000,      # Matches the number of movies in the movies table
                "rating": 10,          # Ratings are distinct values (e.g., 1-10)
                "description": 4500    # Number of unique review descriptions
            }
        },
        "directors": {
            "n_r": 200,                # Total number of directors
            "b_r": 10,
            "l_r": 512,
            "f_r": 20,
            "v_a_r": {
                "director_id": 200,    # Each director has a unique ID
                "name": 180            # Number of unique director names
            }
        },
        "actors": {
            "n_r": 3000,               # Total number of actors
            "b_r": 150,
            "l_r": 512,
            "f_r": 20,
            "v_a_r": {
                "actor_id": 3000,      # Each actor has a unique ID
                "name": 2900           # Number of unique actor names
            }
        },
        "awards": {
            "n_r": 500,
            "b_r": 25,
            "l_r": 256,
            "f_r": 20,
            "v_a_r": {
                "award_id": 500,
                "award_name": 450,
                "movie_id": 1000,      # Matches the number of movies (FK relationship)
            }
        },
        "movie_actors": {              # Linking table for movies and actors
            "n_r": 5000,               # Total number of movie-actor relationships
            "b_r": 100,
            "l_r": 128,
            "f_r": 40,
            "v_a_r": {
                "movie_id": 1000,      # Matches the number of movies
                "actor_id": 3000       # Matches the number of actors
            }
        },
        "movie_directors": {           # Linking table for movies and directors
            "n_r": 1000,               # Total number of movie-director relationships
            "b_r": 50,
            "l_r": 128,
            "f_r": 20,
            "v_a_r": {
                "movie_id": 1000,      # Matches the number of movies
                "director_id": 200     # Matches the number of directors
            }
        }
            }
    return stats