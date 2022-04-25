import pandas as pd
from google.cloud import bigquery
import json


from pylathedb.utils import ConfigHandler,get_logger

from .database_iter import DatabaseIter

logger = get_logger(__name__)

gcp_project = "zeta-instrument-340221"
bq_dataset = "imdb"
table_path = f"{gcp_project}.{bq_dataset}"


class DatabaseHandler:
    def __init__(self,config):
        self.config = config


    def get_tables_and_attributes(self):
        client = bigquery.Client(project=gcp_project)
        client.dataset(bq_dataset)

        sql = f'''
                SELECT info.table_name as table, info.column_name as column
                FROM {table_path}.INFORMATION_SCHEMA.COLUMNS AS info
                WHERE info.table_schema="{bq_dataset}"
                ORDER BY 1,2
                '''

        query = client.query(sql)
        result = query.result()

        tables_attributes = {}
        for row in result:
            if row.table not in tables_attributes:
                tables_attributes[row.table] = [row.column]
            else:
                tables_attributes[row.table].append(row.column)
        return tables_attributes

    def iterate_over_keywords(self,schema_index,**kwargs):
        database_table_columns=schema_index.tables_attributes()
        return DatabaseIter(self.config,database_table_columns,**kwargs)

    def get_fk_constraints(self):
        tables_attributes = None
        fk_constraints = {}

        try:
            if self.config.schema_filepath is not None:
                print('Get database schema from schema_filepath')
                with open(self.config.schema_filepath,'r') as schema_filepath:
                    tables_attributes = [
                        (item['table'],item['attributes'], item['pk'], item['fk'])
                        for item in json.load(schema_filepath) 
                    ]
        except AttributeError as err:
            raise("Missing 'schema_filepath' key on './config' path's file")

        for table, attributes, pk, fks in tables_attributes:
            #print(f"{table} - {pk} - {fks}")
            for fk_data in fks:
                fk = fk_data["field"]
                foreign_table = fk_data["data"]["table"]
                foreign_field = fk_data["data"]["field"]
                cardinality = fk_data["cardinality"]
                constraint = (f"{fk}_exists",table)

                fk_constraints.setdefault((f"{fk}_exists",table),(table, foreign_table, [], []))
                fk_constraints[constraint][2].append((fk, foreign_field)) #adiciona mapeamento de relações
                fk_constraints[constraint][3].append((fk, cardinality)) #adiciona mapeamento de cardinalidade

        for constraint in fk_constraints:
            table,foreign_table,attribute_mappings, cardinality = fk_constraints[constraint]
            fk_constraints[constraint] = (cardinality,table,foreign_table,attribute_mappings)

        return fk_constraints

    def exec_sql (self,sql,**kwargs):
        show_results=kwargs.get('show_results',True)

        client = bigquery.Client(project=gcp_project)
        client.dataset(bq_dataset)

        query = client.query(sql)
        results = query.result()

        #df = results.to_dataframe()
        return results

    def get_dataframe(self,sql,**kwargs):
        client = bigquery.Client(project=gcp_project)
        client.dataset(bq_dataset)

        query = client.query(sql)
        df = query.result().to_dataframe()
        return df


    def exist_results(self,sql):
        sql = f"SELECT EXISTS ({sql.rstrip(';')});"

        client = bigquery.Client(project=gcp_project)
        client.dataset(bq_dataset)

        query = client.query(sql)
        results = query.result()
        return results

