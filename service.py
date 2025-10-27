from pymongo import MongoClient
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
import time
import redis
import numpy as np
from pymongo import ASCENDING, DESCENDING
import json
import calendar
from sentence_transformers import SentenceTransformer
from redis.commands.search.query import Query
from neo4j import GraphDatabase

client = MongoClient("mongodb://localhost:27017/")
db = client.mydb
movies_col = db.movies
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
r_vec = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
movies_col.create_index(
    [("genres", ASCENDING), ("vote_count", ASCENDING), ("vote_average", DESCENDING)],
    name="idx_same_genres_vote"
)

try:
    model = SentenceTransformer("avsolatorio/GIST-small-Embedding-v0")
    print("SentenceTransformer model loaded.")
except Exception as e:
    print(f"Error loading SentenceTransformer model: {e}")
    model = None

REDIS_INDEX_NAME = "plot_vector_idx"

NEO4J_URI = "neo4j+s://6120e762.databases.neo4j.io" 
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "0qhEYtJQRHYFDi76O7ZOxSKPHw5ywNfi6Ba0Vqc9NVM" 

try:
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    neo4j_driver.verify_connectivity()
    print("Successfully connected to Neo4j.")
except Exception as e:
    print(f"Failed to connect to Neo4j: {e}", file=sys.stderr)
    neo4j_driver = None

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
            "$match": {
                "$text": {
                    "$search": text 
                }
            }
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
                "searchResults": [
                    {
                        "$project": {
                            "_id": 1,
                            "title": 1,
                            "poster_path": 1,
                            "release_date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$release_date"}},
                            "vote_average": 1, 
                            "vote_count": 1,
                            "popularity": 1,
                            "weightedScore": 1,
                        }
                    }
                ],
                "genreFacet": [
                    {"$unwind": "$genres"},
                    {"$sortByCount": "$genres"}
                ],
                "releaseYearFacet": [
                    {
                        "$addFields": {
                            "year": {"$year": "$release_date"}
                        }
                    },
                    {"$sortByCount": "$year"}
                ],
                "votesFacet": [
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
        return {"searchResults": [], "genreFacet": [], "releaseYearFacet": [], "votesFacet": []}

@measure
def get_top_rated_movies(top_n=25, min_votes=5000):
    """
    Return top rated 25 movies with more than 5k votes
    """
    cache_key = f"top_rated:v1:n{top_n}:min{min_votes}" 
    
    cached = r.get(cache_key)
    print("HIT" if cached else "MISS", cache_key)
    if cached:
        return json.loads(cached)


    projection = {
            "_id": 1,
            "title": 1,
            "poster_path": 1,
            "release_date": 1,
            "vote_average": 1, 
            "vote_count": 1,
            "popularity": 1,
            "weightedScore": 1,
            "rating":1
        }    
    cursor = (
            movies_col
            .find({"vote_count": {"$gt": min_votes}}, projection)
            .sort([("vote_average", DESCENDING), ("_id", 1)])
            .limit(top_n)
    )
    docs = [parse_movie_document(d) for d in cursor]

    r.setex(cache_key, 300, json.dumps(docs))

    return docs


@measure
def get_recent_released_movies(min_reviews=50, recent_days=800):
    """
    Return recently released movies that at least are reviewed by 50 users
    """
    cache_key = f"recent_released:v1:min{min_reviews}:days{recent_days}"
    
    cached = r.get(cache_key)
    print("HIT" if cached else "MISS", cache_key)
    if cached:
        return json.loads(cached)

    cutoff_date = datetime.utcnow() - timedelta(days=recent_days)
    
    projection = {
            "_id": 1,
            "title": 1,
            "poster_path": 1,
            "release_date": 1,
            "vote_average": 1, 
            "vote_count": 1,
            "popularity": 1,
            "weightedScore": 1,
            "rating":1
        }    


    
    cursor = movies_col.find({
        "release_date": {"$gte": cutoff_date},
        "vote_count": {"$gte": min_reviews}}).sort("release_date", -1)

    docs = [parse_movie_document(d) for d in cursor]

    r.setex(cache_key, 300, json.dumps(docs))

    return list(cursor)

@measure
def get_movie_details(movie_id):
    """
    Return detailed information for the specified movie_id
    """
    movie_id_str = str(movie_id)
    cache_key = f"movie:details:v1:{movie_id_str}"
    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)

    projection = {
        "_id": 1,
        "poster_path":1,
        "title": 1,
        "vote_average": 1,
        "vote_count": 1,
        "release_date": 1,
        "tagline": 1,
        "genres": 1,
        "overview": 1
    }


    doc = movies_col.find_one({"_id": movie_id}, projection)
    
    movie = parse_movie_document(doc)

    if movie:
        r.setex(cache_key, 600, json.dumps(movie))

    return movie


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
    
    genres_key=";".join(sorted(genres))

    

    cache_key=f"same_genres:v1:{movie_id}:{genres_key}"
    cached=r.get(cache_key)
    if cached:
        return json.loads(cached)


    pipeline = [
        {"$match": {
            "genres": {"$in": genres},
            "_id": {"$ne": movie_id},
            "vote_count": {"$gte": 500}
        }},
        {"$addFields": {
            "matchCount": {"$size": {"$setIntersection": ["$genres", genres]}}
        }},
        {"$sort": {"matchCount": -1, "rating": -1, "_id": 1}},
        {"$limit": 8},
        {"$project": {
            "_id": 1, "title": 1, "vote_count": 1, "vote_average":1,
            "poster_path": 1, "release_date": 1, "weightedScore":1,"popularity":1
        }}
    ]


    docs = list(movies_col.aggregate(pipeline))
    
    movies = [parse_movie_document(d) for d in docs]

    r.setex(cache_key, 300, json.dumps(movies, default=lambda o: o.isoformat() if isinstance(o, datetime) else o))
    return movies

@measure
def get_similar_movies(movie_id):
    """
    Return a list of movies with a similar plot as the given movie_id.
    Movies need to be sorted by the popularity instead of proximity score.
    """
    if model is None:
        print("Model not loaded, cannot find similar movies.")
        return []

    K = 10  # Number of similar movies to find

    cache_key = f"similar_movies:v2:{movie_id}:k{K}"

    # 1) Cache HIT?
    cached = r.get(cache_key)
    print("HIT" if cached else "MISS", cache_key)
    if cached:
        return json.loads(cached)

    TARGET_MOVIE_KEY = f"movie:{movie_id}"
    try:
        # 1. Get the embedding for the target movie
        # Use r_vec to get raw bytes
        target_embedding_bytes = r_vec.hget(TARGET_MOVIE_KEY, "embedding")

        if target_embedding_bytes:
            # Movie is in Redis, use its stored embedding
            query_vector = target_embedding_bytes
        else:
            # Movie not in Redis (e.g., < 500 votes).
            # Fetch from Mongo, generate embedding on the fly.
            print(f"Movie {movie_id} not in Redis. Generating embedding on the fly.")
            
            # movie_id from URL is likely an int, which matches _id
            movie = movies_col.find_one({"_id": movie_id})
            if not movie:
                return []
                
            plot = movie.get("plot") or movie.get("overview")
            if not plot:
                return []
                
            # Generate embedding
            embedding = model.encode(plot, normalize_embeddings=True)
            query_vector = embedding.astype(np.float32).tobytes()

        # 2. Build the KNN query
        # This query finds the K+1 nearest neighbors (to include the movie itself)
        query_string = f"*=>[KNN {K+1} @embedding $vec AS score]"
        
        
        q = (
            Query(query_string)
            .sort_by("score") # Sort by similarity score (ascending)
            .return_field("movie_id") # Ask for the _id field
            .return_field("score")
            .dialect(2) # Use DIALECT 2 for KNN queries
        )
        query_params = {"vec": query_vector}
        
        # 3. Execute the query using the r_vec connection
        rs = r_vec.ft(REDIS_INDEX_NAME)

        
        results = rs.search(q, query_params=query_params)
        


        # 4. Process results
        similar_movie_ids = []
        for doc in results.docs:
            # doc._id will be bytes, so decode it


            doc_id_str = doc.movie_id

            # Exclude the movie itself from the results
            if doc_id_str != str(movie_id):
                try:
                    similar_movie_ids.append(int(doc_id_str))
                except ValueError:
                    continue # Skip if _id is invalid
        
        if not similar_movie_ids:
            return []

        # 5. Fetch movie details from MongoDB
        # We use an aggregation pipeline to fetch movies in the order
        # returned by Redis (most similar first).

        pipeline = [
            {"$match": {"_id": {"$in": similar_movie_ids}}},
            # Add a field to preserve the Redis sort order
            {"$addFields": {
                "__order": {"$indexOfArray": [similar_movie_ids, "$_id"]}
            }},
            {"$sort": {"__order": 1}},
            # Project the fields needed for the carousel
            {"$project": {
                "_id": 1,
                "title": 1,
                "poster_path": 1,
                "release_date": 1,
                "vote_average": 1, 
                "vote_count": 1,
                "popularity": 1,
                "weightedScore": 1,
                "rating":1
            }}
        ]
        
        similar_movies = list(movies_col.aggregate(pipeline))
        
        movies = [parse_movie_document(d) for d in similar_movies]

        r.setex(cache_key, 600, json.dumps(movies))
        return movies

    except Exception as e:
        print(f"Error in get_similar_movies: {e}")
        return [] # Return empty list on error


@measure
def get_movie_likes(username, movie_id):
    """
    Returns a list of usernames of users who also like the specified movie_id
    """
    if not neo4j_driver:
        print("Neo4j driver not available.")
        return []

    cache_key = f"movie_likes:v1:u:{username}:m:{movie_id}"
    cached = r.get(cache_key)
    print("HIT" if cached else "MISS", cache_key)
    if cached:
        return json.loads(cached)

    # Ensure movie_id is an integer for the query
    try:
        movie_id_int = int(movie_id)
    except (ValueError, TypeError):
        return [] # Return empty if movie_id is invalid

    query = """
    // Find the movie by its tmdb_id
    MATCH (m:Movie {tmdb_id: $movie_id})
    
    // Find users who like that movie
    MATCH (m)<-[:LIKES]-(u:User)
    
    // Exclude the original user
    WHERE u.username <> $username
    
    // Return their usernames, up to a limit of 10
    RETURN u.username AS username
    LIMIT 10
    """
    
    try:
        with neo4j_driver.session() as session:
            result = session.run(query, movie_id=movie_id_int, username=username)
            # Collect usernames from the result records
            usernames = [record["username"] for record in result]
            r.setex(cache_key, 300, json.dumps(usernames))
            return usernames
    except Exception as e:
        print(f"Error in get_movie_likes: {e}")
        return []


@measure
def get_recommendations_for_user(username):
    """
    Return up to 10 movies based on similar users taste.
    """
    if not neo4j_driver:
        print("Neo4j driver not available.")
        return []

    cache_key = f"reco_user:v1:{username}"
    cached = r.get(cache_key)
    print("HIT" if cached else "MISS", cache_key)
    if cached:
        return json.loads(cached)

    # This Cypher query implements all 5 steps from the project description:
    # 1. Find users with common likes
    # 2. Compute Jaccard similarity
    # 3. Select top K neighbors
    # 4. Find movies neighbors liked (that active user has not)
    # 5. Rank movies by likes
    query = """
    // 1. Find the active user and their liked movies
    MATCH (activeUser:User {username: $username})-[:LIKES]->(activeMovie:Movie)
    WITH activeUser, collect(activeMovie) AS activeUserMovies

    // 2. Find neighbors who share at least one movie
    MATCH (neighbor:User)-[:LIKES]->(commonMovie:Movie)
    WHERE neighbor <> activeUser AND commonMovie IN activeUserMovies
    WITH activeUser, activeUserMovies, neighbor, collect(commonMovie) AS commonMovies

    // 3. Get all movies for the neighbor
    MATCH (neighbor)-[:LIKES]->(neighborMovie:Movie)
    WITH activeUser, activeUserMovies, neighbor, commonMovies, collect(neighborMovie) AS neighborMovies

    // 4. Calculate Jaccard similarity
    WITH activeUser, 
         activeUserMovies, 
         neighbor, 
         size(commonMovies) AS intersection,
         size(activeUserMovies) + size(neighborMovies) - size(commonMovies) AS union
    WHERE union > 0 // Avoid division by zero
    WITH activeUser, 
         activeUserMovies, 
         neighbor, 
         toFloat(intersection) / toFloat(union) AS jaccard
         
    // 5. Get top K neighbors (k=10)
    ORDER BY jaccard DESC
    LIMIT 10 
    WITH activeUser, activeUserMovies, collect(neighbor) AS topNeighbors

    // 6. Unwind the neighbors list
    UNWIND topNeighbors AS topNeighbor

    // 7. Find movies liked by these neighbors
    MATCH (topNeighbor)-[:LIKES]->(recommendedMovie:Movie)

    // 8. Filter out movies the active user already likes
    WHERE NOT recommendedMovie IN activeUserMovies

    // 9. Rank by number of likes from the neighbor group
    WITH recommendedMovie, count(recommendedMovie) AS recommendationScore
    ORDER BY recommendationScore DESC
    LIMIT 10 // Top 10 movie recommendations

    // 10. Return just the movie ID (which is the _id in MongoDB)
    RETURN recommendedMovie.tmdb_id AS _id
    """
    
    recommended_movie_ids = []
    try:
        # Execute the query to get ordered movie IDs
        with neo4j_driver.session() as session:
            result = session.run(query, username=username)
            recommended_movie_ids = [record["_id"] for record in result]
            
    except Exception as e:
        print(f"Error in get_recommendations_for_user (Neo4j query): {e}")
        return []

    if not recommended_movie_ids:
        # No recommendations found
        return []

    # Now, fetch the full movie details from MongoDB,
    # preserving the order from the Neo4j query.
    try:
        pipeline = [
            {"$match": {"_id": {"$in": recommended_movie_ids}}},
            # Add a field to preserve the Neo4j sort order
            {"$addFields": {
                "__order": {"$indexOfArray": [recommended_movie_ids, "$_id"]}
            }},
            {"$sort": {"__order": 1}},
            # Project to match the format of other functions
            {"$project": {
                "_id": 1,
                "title": 1,
                "poster_path": 1,
                "release_date": 1,
                "vote_average": 1,
                "vote_count": 1
            }}
        ]
        
        recommended_movies = list(movies_col.aggregate(pipeline))

        for movie in recommended_movies:
            if isinstance(movie.get("release_date"), datetime):
                movie["release_date"] = movie["release_date"].strftime("%Y-%m-%d")
            movie["_id"] = str(movie["_id"])
        
        r.setex(cache_key, 600, json.dumps(recommended_movies))

        return recommended_movies

    except Exception as e:
        print(f"Error in get_recommendations_for_user (MongoDB query): {e}")
        return []


def get_metrics(metrics_names: List) -> Dict[str, Tuple[float, float]]:
    """
    Return 95th percentile in seconds for each one of the given metric names
    """
    results = {}
    for name in metrics_names:
        key = f"metric:{name}"
        
        # 1. Retrieve all stored measurements (as strings)
        raw_measures = r.lrange(key, 0, -1)
        
        if not raw_measures:
            results[name] = (0.0, 0.0)
            continue

        # 2. Convert to float array for NumPy calculation
        measures = np.array([float(m) for m in raw_measures])
        
        # 3. Calculate 90th and 95th percentiles
        p90 = np.percentile(measures, 90)
        p95 = np.percentile(measures, 95)
        
        # Return percentiles as a tuple of floats (ms)
        results[name] = (p90, p95)
        
    return results


def store_metric(metric_name: str, measure_s: float):
    """
    Store mesured sample in seconds of the given metric
    """
    r.rpush(f"metric:{metric_name}", measure_s) # Store in milliseconds for clarity


def parse_movie_document(doc):
    """
    Normalitza un document de pel·lícula (find o aggregate) perquè sigui
    JSON-serialitzable i consistent a tota l'app.
    """
    if not doc:
        return None

    # --- release_date a string ---
    rd = doc.get("release_date")
    if isinstance(rd, datetime):
        rd = rd.strftime("%Y-%m-%d")
    elif isinstance(rd, (int, float)):
        try:
            rd = datetime.fromtimestamp(rd).strftime("%Y-%m-%d")
        except Exception:
            print("Error in the date.")
            rd = None

    va = doc.get("vote_average")
    if va is None and doc.get("rating") is not None:
        va = float(doc.get("rating"))
    movie = {
        "_id": str(doc.get("_id")),
        "title": doc.get("title"),
        "poster_path": doc.get("poster_path"),
        "release_date": rd if rd else None,
        "vote_average": float(va) if va is not None else None,
        "vote_count": int(doc.get("vote_count", 0)) if doc.get("vote_count") is not None else None,
        "popularity": float(doc.get("popularity")) if doc.get("popularity") is not None else None,
        "weightedScore": float(doc.get("weightedScore")) if doc.get("weightedScore") is not None else None,
        "tagline": doc.get("tagline"),
        "genres": doc.get("genres"),
        "overview": doc.get("overview"),
    }

    return movie

