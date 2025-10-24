import csv
import sys
from neo4j import GraphDatabase
from pymongo import MongoClient

# --- Configuration (FILL IN ALL FOUR) ---

# 1. Neo4j Connection Details
NEO4J_URI = "neo4j+s://adeab0fe.databases.neo4j.io" # Your Neo4j Aura URI
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "z_-YvxqcgFgxRdWV9O4CmQmU8LH3_sRodpmeHY6GyAU" 

# 2. MongoDB Connection Details
MONGO_URI = "mongodb://localhost:27017/"  # Default MongoDB URI
MONGO_DB_NAME = "mydb"                    # From your services.py
MONGO_COLLECTION_NAME = "movies"          # From your services.py

# 3. File Path
CSV_FILE_PATH = "movies_likes.csv"

class Neo4jImporter:
    def __init__(self, uri, user, password, mongo_uri, mongo_db, mongo_coll):
        # Connect to Neo4j
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            self.driver.verify_connectivity()
            print(f"Successfully connected to Neo4j at {uri}")
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}", file=sys.stderr)
            sys.exit(1)
            
        # Connect to MongoDB
        try:
            self.mongo_client = MongoClient(mongo_uri)
            self.db = self.mongo_client[mongo_db]
            self.movies_col = self.db[mongo_coll]
            # Test connection
            self.movies_col.find_one({"_id": 1}, {"_id": 1})
            print(f"Successfully connected to MongoDB at {mongo_uri}")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}", file=sys.stderr)
            sys.exit(1)

    def close(self):
        self.driver.close()
        print("Neo4j connection closed.")
        self.mongo_client.close()
        print("MongoDB connection closed.")

    def create_constraints(self):
        """Creates unique constraints for User and Movie nodes."""
        print("Creating constraints...")
        with self.driver.session() as session:
            try:
                session.run("CREATE CONSTRAINT user_username_unique IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE")
                session.run("CREATE CONSTRAINT movie_tmdb_id_unique IF NOT EXISTS FOR (m:Movie) REQUIRE m.tmdb_id IS UNIQUE")
                print("Constraints created successfully.")
            except Exception as e:
                print(f"Error creating constraints: {e}", file=sys.stderr)

    def import_data(self, csv_path):
        """
        Reads the survey CSV, fetches movie titles from MongoDB,
        and imports all data into Neo4j.
        """
        print(f"Starting import from {csv_path}...")
        total_relationships = 0
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                # Use standard csv.reader, not DictReader, due to file format
                reader = csv.reader(f) 
                
                # Skip the header row
                try:
                    next(reader) 
                except StopIteration:
                    print("Error: CSV file is empty.", file=sys.stderr)
                    return

                # Process each user row from the CSV
                for row in reader:
                    if not row or len(row) < 3:
                        continue # Skip empty or malformed rows

                    username = row[1].strip()
                    movie_ids_str = row[2].strip()
                    
                    if not username or not movie_ids_str:
                        print(f"Skipping row with missing data: {row}")
                        continue
                    
                    # 1. Parse movie IDs from the string
                    try:
                        movie_ids = [int(mid.strip()) for mid in movie_ids_str.split(',') if mid.strip()]
                    except ValueError as e:
                        print(f"Skipping user {username}, invalid movie ID: {e}", file=sys.stderr)
                        continue
                    
                    # 2. Fetch movie titles from MongoDB
                    movies_from_db = self.movies_col.find({"_id": {"$in": movie_ids}}, {"title": 1})
                    
                    # Create a lookup map of {id: title}
                    title_map = {m["_id"]: m["title"] for m in movies_from_db if m.get("title")}

                    # 3. Prepare the batch for Neo4j
                    batch = []
                    for mid in movie_ids:
                        if mid in title_map:
                            batch.append({
                                "username": username,
                                "tmdb_id": mid,
                                "title": title_map[mid]
                            })
                        else:
                            print(f"Warning: Movie ID {mid} (for user {username}) not found in MongoDB. Skipping this 'like'.")
                    
                    # 4. Run the Neo4j import for this user's batch
                    if batch:
                        self.run_batch(batch)
                        total_relationships += len(batch)
                        print(f"Imported {len(batch)} relationships for user: {username}")
                
                print(f"\nImport complete. Total relationships imported: {total_relationships}.")

        except FileNotFoundError:
            print(f"Error: The file '{csv_path}' was not found.", file=sys.stderr)
        except Exception as e:
            print(f"An error occurred during import: {e}", file=sys.stderr)

    def run_batch(self, batch):
        """
        Executes a single batch import query.
        """
        query = """
        UNWIND $batch AS row
        MERGE (u:User {username: row.username})
        MERGE (m:Movie {tmdb_id: row.tmdb_id})
        ON CREATE SET m.title = row.title
        MERGE (u)-[r:LIKES]->(m)
        """
        with self.driver.session() as session:
            session.run(query, batch=batch)

if __name__ == "__main__":
    importer = None
    try:
        importer = Neo4jImporter(
            NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
            MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME
        )
        importer.create_constraints()
        importer.import_data(CSV_FILE_PATH)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if importer:
            importer.close()
