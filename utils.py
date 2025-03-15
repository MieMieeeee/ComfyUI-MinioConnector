import datetime
import hashlib

LOGO_SUFFIX = "|Mie"
LOGO_EMOJI = "🐑"


def mie_log(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    the_message = f"[{timestamp}] {LOGO_EMOJI}: {message}"
    print(the_message)
    return the_message


def add_suffix(source):
    return source + LOGO_SUFFIX


def add_emoji(source):
    return source + " " + LOGO_EMOJI


def calculate_file_hash(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
