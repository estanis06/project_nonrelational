import pymongo
import redis
from redis.commands.search.field import VectorField, TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from sentence_transformers import SentenceTransformer
import numpy as np
import sys

# --- Configuration (matches your services.py) ---
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "mydb"  # From your services.py
MONGO_COLLECTION = "movies" # From your services.py
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_INDEX_NAME = "plot_vector_idx"
MODEL_NAME = "avsolatorio/GIST-small-Embedding-v0"
VECTOR_DIMENSIONS = 384 # As specified by the model
MOVIE_PREFIX = "movie:" # Prefix for Redis keys

# --- 1. Initialize Connections and Model ---

try:
    # Connect to MongoDB
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    movies_collection = db[MONGO_COLLECTION]
    print("Successfully connected to MongoDB (mydb.movies).")

    # Connect to Redis (NOTE: decode_responses=False for raw bytes)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=False)
    r.ping()
    print("Successfully connected to Redis.")

    # Load Sentence Transformer Model
    print(f"Loading embedding model: {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)
    print("Model loaded.")

except Exception as e:
    print(f"Error during initialization: {e}")
    sys.exit(1)


# --- 2. Define and Create Redis Search Index ---

def create_redis_index():
    """Defines and creates the vector search index in Redis."""
    schema = (
        # Field for the movie ID (using _id from your collection)
        # We'll store it as 'movie_id' in the hash
        TextField("movie_id", as_name="movie_id"), 
        
        # Field for the vector embedding
        VectorField(
            "embedding",
            "FLAT", # Using a flat index
            {
                "TYPE": "FLOAT32",
                "DIM": VECTOR_DIMENSIONS,
                "DISTANCE_METRIC": "COSINE", # Using cosine similarity
            },
            as_name="embedding",
        ),
    )
    
    # Index definition: we're indexing Hashes with keys starting with MOVIE_PREFIX
    definition = IndexDefinition(prefix=[MOVIE_PREFIX], index_type=IndexType.HASH)

    # Get the search index client
    rs = r.ft(REDIS_INDEX_NAME)

    try:
        # Clean up old index if it exists, for a fresh start
        print(f"Dropping old index '{REDIS_INDEX_NAME}' if it exists...")
        rs.dropindex(delete_documents=True)
        print("Old index dropped.")
    except redis.exceptions.ResponseError:
        print("No old index to drop.")

    try:
        # Create the new index
        print(f"Creating index '{REDIS_INDEX_NAME}'...")
        rs.create_index(fields=schema, definition=definition)
        print("Index created.")
    except Exception as e:
        print(f"Error creating index: {e}")
        sys.exit(1)

create_redis_index()


# --- 3. Populate Redis with Movie Embeddings ---

def populate_embeddings():
    """Fetches movies, generates embeddings, and stores them in Redis."""
    print("Starting movie population...")
    
    # Fetch movies with vote_count > 500, projecting only needed fields
    movies_to_index = movies_collection.find(
        {"vote_count": {"$gt": 500}},
        {"_id": 1, "plot": 1, "overview": 1} # Use _id as the identifier
    )

    count = 0
    # Use a Redis pipeline for efficient batch insertion
    pipeline = r.pipeline(transaction=False)
    
    for movie in movies_to_index:
        # Use plot, fallback to overview
        plot = movie.get("plot") or movie.get("overview")
        movie_id = movie.get("_id") # Use _id from your collection

        # Skip if no plot or movie ID
        if not plot or not movie_id:
            continue
        
        # Ensure movie_id is a string for the hash
        movie_id_str = str(movie_id)

        try:
            # Generate embedding
            embedding = model.encode(plot, normalize_embeddings=True)
            
            # Convert to numpy float32 bytes for Redis
            embedding_bytes = embedding.astype(np.float32).tobytes()

            # Prepare the hash data
            movie_hash = {
                "movie_id": movie_id_str,
                "embedding": embedding_bytes
            }
            
            # Add to pipeline
            redis_key = f"{MOVIE_PREFIX}{movie_id_str}"
            pipeline.hset(redis_key, mapping=movie_hash)
            
            count += 1
            if count % 100 == 0:
                print(f"Processed {count} movies...")
                # Execute pipeline in batches of 100
                pipeline.execute()

        except Exception as e:
            print(f"Error processing movie {movie_id_str}: {e}")

    # Execute any remaining items in the pipeline
    pipeline.execute()
    print(f"\nPopulation complete. Total movies indexed: {count}.")

populate_embeddings()
mongo_client.close()
print("MongoDB connection closed.")