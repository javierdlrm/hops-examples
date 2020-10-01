import os

# Env names
HOST = "FS_HOST"
PORT = "FS_PORT"
PROJECT = "FS_PROJECT"
API_KEY = "FS_API_KEY"
SECRETS_STORE = "FS_SECRETS_STORE"
STORAGE = "FS_STORAGE"

# Defaults
DEFAULT_PORT = "8181"
DEFAULT_SECRETS_STORE = "local"
DEFAULT_STORAGE = "online"


class OnlineFSConfig:
    def __init__(self):
        self.read_from_env()

    def read_from_env(self):
        self.host = os.environ[HOST]
        self.port = os.getenv(PORT, DEFAULT_PORT)
        self.project = os.environ[PROJECT]
        self.secrets_store = os.getenv(SECRETS_STORE, DEFAULT_SECRETS_STORE)
        self.api_key = os.environ[API_KEY]
        self.storage = os.getenv(STORAGE, DEFAULT_STORAGE)
