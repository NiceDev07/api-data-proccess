from typing import Union
from fastapi import FastAPI
from config import settings

app = FastAPI()
print(settings.ENV)