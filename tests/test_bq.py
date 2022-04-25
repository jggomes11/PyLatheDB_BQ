import pandas as pd
from google.cloud import bigquery

import nltk
nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('omw-1.4')

from pylathedb.lathe import Lathe
from pylathedb.evaluation import EvaluationHandler
from pylathedb.utils import ConfigHandler
from pylathedb.database import DatabaseHandler
from pylathedb.index import IndexHandler

lathe = Lathe()

# Choose Queryset
lathe.change_queryset(1) #IMDB Big Query

lathe.max_qm_size = 3
lathe.max_cjn_size = 5
lathe.topk_cns = 10
lathe.configuration = (5,1,9)

print("\n--- Indexes Phase ---")
lathe.create_indexes()
lathe.load_indexes()

print("\n--- Search Phase ---")

lathe.keyword_search()
result = lathe.keyword_search("julia roberts films")
result.cjns(text=True,graph=False,sql=True,df=False)

'''
results = lathe.run_queryset(export_results=False)

lathe.evaluation_handler.evaluate_results(
    results,
)
'''
