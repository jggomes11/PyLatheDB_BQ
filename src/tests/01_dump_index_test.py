import sys
import os
sys.path.append('../')


from utils import ConfigHandler
from index import IndexHandler

config = ConfigHandler()
indexHandler = IndexHandler()

print(os.path.abspath(__file__))
indexHandler.create_indexes()
