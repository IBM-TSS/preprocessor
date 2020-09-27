import os


# MongoDB env vars
DB_USER = os.environ.get('DB_USER', '')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '')
SHORT_RUN_DB = os.environ.get('SHORT_RUN_DB')


def get_mongo_url():
    ''' Returns proper mongo connection URL from env file. '''
    return f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.9byad.mongodb.net/test?retryWrites=true&w=majority"
