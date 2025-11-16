import os
import dotenv
import pathlib

dotenv.load_dotenv(pathlib.Path(__file__).parent.joinpath(".env"))

KABU_PLUS_USER = os.environ.get("KABU_PLUS_USER")
KABU_PLUS_PASS = os.environ.get("KABU_PLUS_PASS")
