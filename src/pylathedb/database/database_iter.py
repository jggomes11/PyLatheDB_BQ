from pylathedb.utils import ConfigHandler, get_logger, Tokenizer
from google.cloud import bigquery


gcp_project = "zeta-instrument-340221"
bq_dataset = "imdb"
table_path = f"{gcp_project}.{bq_dataset}"

logger = get_logger(__name__)
class DatabaseIter:
    def __init__(self,config,database_table_columns, **kwargs):
        self.limit_per_table = kwargs.get('limit_per_table', None)
        self.tokenizer = kwargs.get('tokenizer', Tokenizer())
        self.config = config
        self.database_table_columns=database_table_columns
        self.table_hash = self._get_indexable_schema_elements()

    def _schema_element_validator(self,table,column):
        return True

    def _get_indexable_schema_elements(self):
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

        # Get data from gib query table
        tables_attributes = {}
        for row in result:
            if row.table not in tables_attributes:
                tables_attributes[row.table] = [row.column]
            else:
                tables_attributes[row.table].append(row.column)

        table_hash = {}
        for table,columns in tables_attributes.items():
            #print(f"{table} - {column}")
            for column in columns:
                if (table,column) not in self.database_table_columns: # this compare here is not necessary for big query, but for code propose we create the code up here
                    print(f'SKIPPED {(table,column)}')
                    continue
                table_hash.setdefault(table,[]).append(column)
        return table_hash

    def __iter__(self):
        for table,columns in self.table_hash.items():
            indexable_columns = [col for col in columns if self._schema_element_validator(table,col)]
            if len(indexable_columns)==0:
                continue

            print('\nINDEXING {}({})'.format(table,', '.join(indexable_columns)))
            client = bigquery.Client(project=gcp_project)
            client.dataset(bq_dataset)


            if self.limit_per_table is not None:
                text = (f"SELECT id, {{}} FROM {{}} LIMIT {self.limit_per_table};")
            else:
                fields = ", ".join(columns)              
                text = f'''
                        SELECT {fields}
                        FROM `{table_path}.{table}`
                        '''
            
            query = client.query(text)
            result = query.result().to_dataframe()

            def formatter(original):
                if original == None:
                    return ""
                elif original == 0:
                    return "False"
                elif original == 1:
                    return "True"
                else:
                    return f"{original}"

            for row in result.index:
                #print(f'\nRow: {row}')
                for col in result.columns:
                    #print(f"\nCol: {col}")
                    ctid = result[col][row]
                    text = formatter(result[col][row])
                    #print(f"Text: {text}")
                    tokens = self.tokenizer.tokenize(text)
                    #print(f"Tokens: {tokens}")
                    for word in tokens:
                        yield table,ctid,col, word
