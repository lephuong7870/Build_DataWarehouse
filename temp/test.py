import sys
import os
from os.path import join, dirname, abspath
from dotenv import load_dotenv

load_dotenv(verbose=True)
dotenv_path = join(dirname(abspath(__name__)), ".env")
print(dotenv_path)
for key, value in os.environ.items():
    print(f"{key}: {value}")

sys.path.insert(0, dirname(dirname(abspath(__file__))))
dag_file_path = dirname(abspath(__file__))
print(dag_file_path)