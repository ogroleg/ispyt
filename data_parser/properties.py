from utils import WINDOWS

MAX_FILE_CACHE_SIZE = 2000  # per process
FILE_CACHE_DELAY = 0.2  # seconds to wait
NUM_RESULTS_TO_SAVE = 25 * 1000
DB_CONNECTION_TIMEOUT = 10 * 1000  # ms
FILE_ENCODING = 'windows-1251' if WINDOWS else 'utf-8'
