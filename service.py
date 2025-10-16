from typing import Dict, List, Tuple
import datetime
import time


# DO NOT MODIFY THIS FUNCTION
def measure(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            end = time.time()
            store_metric(func.__name__, end - start)

    return wrapper


@measure
def search_movie(text):
    """
    Search movies by title and sort the results using a custom score calculated as `textScore * popularity`.
    Also return facets for field `genre`, `releaseYear` and `votes`.

    Hint: check MongoDB's $facet stage
    """
    return {
        "genreFacet": [
            {"_id": "Science Fiction", "count": 21},
            {"_id": "Horror", "count": 10},
            {"_id": "Action", "count": 9},
            # ...
            {"_id": "Drama", "count": 1},
        ],
        "releaseYearFacet": [
            {"_id": 1979, "count": 1},
            {"_id": 1986, "count": 1},
            # ...
            {"_id": 2022, "count": 1},
            {"_id": 2023, "count": 2},
        ],
        "searchResults": [
            {
                "_id": 981314,
                "poster_path": "/kaSvEH3RJvQa6NfAuEVqDMBEk5E.jpg",
                "release_date": datetime.datetime(2023, 5, 11, 0, 0),
                "score": 0.75,
                "title": "Alien Invasion",
                "vote_average": 5.542,
                "vote_count": 48,
            },
            {
                "_id": 126889,
                "poster_path": "/zecMELPbU5YMQpC81Z8ImaaXuf9.jpg",
                "release_date": datetime.datetime(2017, 5, 9, 0, 0),
                "score": 0.75,
                "title": "Alien: Covenant",
                "vote_average": 6.1,
                "vote_count": 7822,
            },
            # ...
        ],
        "votesFacet": [
            {"_id": 0, "count": 2},
            {"_id": 5, "count": 18},
            {"_id": 7, "count": 4},
            {"_id": 8, "count": 1},
        ],
    }


@measure
def get_top_rated_movies():
    """
    Return top rated 25 movies with more than 5k votes
    """
    return [
        {
            "_id": 238,
            "poster_path": "/3bhkrj58Vtu7enYsRolD1fZdja1.jpg",
            "release_date": datetime.datetime(1972, 3, 14, 0, 0),
            "title": "The Godfather",
            "vote_average": 8.707,
            "vote_count": 18677,
        },
        {
            "_id": 278,
            "poster_path": "/lyQBXzOQSuE59IsHyhrp0qIiPAz.jpg",
            "release_date": datetime.datetime(1994, 9, 23, 0, 0),
            "title": "The Shawshank Redemption",
            "vote_average": 8.702,
            "vote_count": 24649,
        },
    ]


@measure
def get_recent_released_movies():
    """
    Return recently released movies that at least are reviewed by 50 users
    """
    return [
        {
            "_id": 1151534,
            "poster_path": "/rpzFxv78UvYG5yQba2soO5mMl4T.jpg",
            "release_date": datetime.datetime(2023, 9, 29, 0, 0),
            "title": "Nowhere",
            "vote_average": 7.895,
            "vote_count": 195,
        },
        {
            "_id": 866463,
            "poster_path": "/soIgqZBoTiTgMqUW0JtxsPWAilQ.jpg",
            "release_date": datetime.datetime(2023, 9, 29, 0, 0),
            "title": "Reptile",
            "vote_average": 7.354,
            "vote_count": 65,
        },
    ]


@measure
def get_movie_details(movie_id):
    """
    Return detailed information for the specified movie_id
    """
    return {
        "_id": 238,
        "genres": ["Drama", "Crime"],
        "overview": "Spanning the years 1945 to 1955, a chronicle of the fictional "
        "Italian-American Corleone crime family. When organized crime "
        "family patriarch, Vito Corleone barely survives an attempt on "
        "his life, his youngest son, Michael steps in to take care of the "
        "would-be killers, launching a campaign of bloody revenge.",
        "poster_path": "/3bhkrj58Vtu7enYsRolD1fZdja1.jpg",
        "release_date": datetime.datetime(1972, 3, 14, 0, 0),
        "tagline": "An offer you can't refuse.",
        "title": "The Godfather",
        "vote_average": 8.707,
        "vote_count": 18677,
    }


@measure
def get_same_genres_movies(movie_id, genres):
    """
    Return a list of movies that match at least one of the provided genres.

    Movies need to be sorted by the number genres that match in descending order
    (a movie matching two genres will appear before a movie only matching one). When
    several movies match with the same number of genres, movies with greater rating must
    appear first.

    Discard movies with votes by less than 500 users. Limit to 8 results.
    """
    return [
        {
            "_id": 335,
            "genres": 2,
            "poster_path": "/qbYgqOczabWNn2XKwgMtVrntD6P.jpg",
            "release_date": datetime.datetime(1968, 12, 21, 0, 0),
            "title": "Once Upon a Time in the West",
            "vote_average": 8.294,
            "vote_count": 3923,
        },
        {
            "_id": 3090,
            "genres": 2,
            "poster_path": "/pWcst7zVbi8Z8W6GFrdNE7HHRxL.jpg",
            "release_date": datetime.datetime(1948, 1, 15, 0, 0),
            "title": "The Treasure of the Sierra Madre",
            "vote_average": 7.976,
            "vote_count": 1066,
        },
    ]


@measure
def get_similar_movies(movie_id):
    """
    Return a list of movies with a similar plot as the given movie_id.
    Movies need to be sorted by the popularity instead of proximity score.
    """
    return [
        {
            "_id": 335,
            "genres": 2,
            "poster_path": "/qbYgqOczabWNn2XKwgMtVrntD6P.jpg",
            "release_date": datetime.datetime(1968, 12, 21, 0, 0),
            "title": "Once Upon a Time in the West",
            "vote_average": 8.294,
            "vote_count": 3923,
        },
        {
            "_id": 3090,
            "genres": 2,
            "poster_path": "/pWcst7zVbi8Z8W6GFrdNE7HHRxL.jpg",
            "release_date": datetime.datetime(1948, 1, 15, 0, 0),
            "title": "The Treasure of the Sierra Madre",
            "vote_average": 7.976,
            "vote_count": 1066,
        },
    ]


@measure
def get_movie_likes(username, movie_id):
    """
    Returns a list of usernames of users who also like the specified movie_id
    """
    return ["username.of.another.student"]


@measure
def get_recommendations_for_user(username):
    """
    Return up to 10 movies based on similar users taste.
    """
    return [
        {
            "_id": 496243,
            "poster_path": "/7IiTTgloJzvGI1TAYymCfbfl3vT.jpg",
            "release_date": datetime.datetime(2019, 5, 30, 0, 0),
            "title": "Parasite",
            "vote_average": 8.515,
            "vote_count": 16430,
        }
    ]


def get_metrics(metrics_names: List) -> Dict[str, Tuple[float, float]]:
    """
    Return 95th percentile in seconds for each one of the given metric names
    """
    return {name: (0.9, 0.95) for name in metrics_names}


def store_metric(metric_name: str, measure_s: float):
    """
    Store mesured sample in seconds of the given metric
    """
    pass
