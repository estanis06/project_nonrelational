from pymongo import MongoClient
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
import time

client = MongoClient("mongodb://localhost:27017/")
db = client.mydb
movies_col = db.movies

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
def search_movie(text,limit=25):
    """
    Search movies by title and sort the results using a custom score calculated as `textScore * popularity`.
    Also return facets for field `genre`, `releaseYear` and `votes`.

    Hint: check MongoDB's $facet stage
    """
    pipeline = [
        {
            "$match": {"title": {"$search": text}}
        },
        {
            "$addFields": {
                "score": {"$multiply": [{"$meta": "textScore"}, "$popularity"]}
            }
        },
        {
            "$sort": {"score": -1}
        },
        {
            "$limit": limit
        },
        {
            "$facet": {
                "results": [
                    {
                        "$project": {
                            "_id": 1,
                            "title": 1,
                            "genres": 1,
                            "release_date": 1,
                            "vote_count": 1,
                            "popularity": 1,
                            "score": 1
                        }
                    }
                ],
                "genre": [
                    {"$unwind": "$genres"},
                    {"$sortByCount": "$genres"}
                ],
                "releaseYear": [
                    {
                        "$addFields": {
                            "year": {"$year": "$release_date"}
                        }
                    },
                    {"$sortByCount": "$year"}
                ],
                "votes": [
                    {
                        "$bucket": {
                            "groupBy": "$vote_count",
                            "boundaries": [0, 50, 100, 500, 1000, 5000, 10000, 50000, 100000],
                            "default": "100000+",
                            "output": {"count": {"$sum": 1}}
                        }
                    }
                ]
            }
        }
    ]

    result = list(movies_col.aggregate(pipeline))
    if result:
        return result[0]
    else:
        return {"results": [], "genre": [], "releaseYear": [], "votes": []}


@measure
def get_top_rated_movies(top_n=25, min_votes=5000):
    """
    Return top rated 25 movies with more than 5k votes
    """
    cursor = movies_col.find({"vote_count": {"$gt": min_votes}}).sort("rating", -1).limit(top_n)
    return list(cursor)


@measure
def get_recent_released_movies(min_reviews=50, recent_days=1200):
    """
    Return recently released movies that at least are reviewed by 50 users
    """
    cutoff_date = datetime.utcnow() - timedelta(days=recent_days)
    cursor = movies_col.find({
        "release_date": {"$gte": cutoff_date},
        "vote_count": {"$gte": min_reviews}
    }).sort("release_date", -1)
    return list(cursor)


@measure
def get_movie_details(movie_id):
    """
    Return detailed information for the specified movie_id
    """
    movie = movies_col.find_one({"_id": movie_id})
    if movie:
        return movie
    else:
        return None


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
    if genres is None:
        movie = movies_col.find_one({"_id": movie_id})
        if not movie or "genres" not in movie:
            return []
        genres = movie["genres"]
    
    cursor = movies_col.find({
        "genres": {"$in": genres},
        "_id": {"$ne": movie_id}
    })
    return list(cursor)


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
            "release_date": datetime(1968, 12, 21, 0, 0),
            "title": "Once Upon a Time in the West",
            "vote_average": 8.294,
            "vote_count": 3923,
        },
        {
            "_id": 3090,
            "genres": 2,
            "poster_path": "/pWcst7zVbi8Z8W6GFrdNE7HHRxL.jpg",
            "release_date": datetime(1948, 1, 15, 0, 0),
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
            "release_date": datetime(2019, 5, 30, 0, 0),
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
