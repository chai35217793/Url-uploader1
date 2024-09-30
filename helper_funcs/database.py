import os
import threading
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Load configuration based on the environment
if bool(os.environ.get("WEBHOOK", False)):
    from sample_config import Config
else:
    from config import Config

# Lock to prevent race conditions during insertion or deletion
INSERTION_LOCK = threading.RLock()

# MongoDB connection setup
def start():
    try:
        # Connect to the MongoDB server
        client = MongoClient(Config.DB_URI)
        
        # Test the connection
        client.admin.command('ping')  # This will throw an exception if the connection fails
        
        # Return the database (assuming your database name is stored in Config.DB_NAME)
        

# Start MongoDB session
db = start()

# Thumbnail collection (assumed to be named 'thumbnails')
thumbnail_collection = db['thumbnails'] if db else None

# Thumbnail Model equivalent for MongoDB
class Thumbnail:
    def __init__(self, id, msg_id):
        self.id = id
        self.msg_id = msg_id

    def to_dict(self):
        return {"_id": self.id, "msg_id": self.msg_id}

# Insert or update thumbnail
async def df_thumb(id, msg_id):
    with INSERTION_LOCK:
        # Check if a thumbnail with the given ID exists
        existing = thumbnail_collection.find_one({"_id": id})
        if existing:
            # Update the existing thumbnail
            thumbnail_collection.update_one({"_id": id}, {"$set": {"msg_id": msg_id}})
        else:
            # Insert a new thumbnail
            new_thumb = Thumbnail(id, msg_id).to_dict()
            thumbnail_collection.insert_one(new_thumb)

# Delete thumbnail by ID
async def del_thumb(id):
    with INSERTION_LOCK:
        # Delete the thumbnail with the given ID
        thumbnail_collection.delete_one({"_id": id})

# Retrieve thumbnail by ID
async def thumb(id):
    try:
        # Find the thumbnail by ID
        t = thumbnail_collection.find_one({"_id": id})
        return t
    except Exception as e:
        print(f"Error retrieving thumbnail: {e}")
    finally:
        # You can close MongoDB connection here if needed, though it's handled by MongoClient automatically.
        pass
