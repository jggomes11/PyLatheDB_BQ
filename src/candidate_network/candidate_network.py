import json
from collections import Counter  #used to check whether two CandidateNetworks are equal
from queue import deque
from pprint import pprint as pp

from keyword_match import KeywordMatch
from utils import Graph

class CandidateNetwork(Graph):

    def __init__(self, graph_dict=None, has_edge_info=False):
        self.score = None
        self.__root = None

        super().__init__(graph_dict,has_edge_info)

        if len(self)>0:
            self.set_root()

    def get_root(self):
        return self.__root

    def set_root(self,vertex=None):
        if len(self) == 0:
            return None

        if vertex is not None:
            keyword_match,alias = vertex
            if not keyword_match.is_free():
                self.__root = vertex
                return vertex
            else:
                return None
        else:         
            for candidate in self.vertices():
                keyword_match,alias = candidate
                if not keyword_match.is_free():
                    self.__root = candidate
                    return candidate
        
        print('The root of a Candidate Network cannot be a Keyword-Free Match.')
        print(self)
        raise ValueError('The root of a Candidate Network cannot be a Keyword-Free Match.')
        
        # return None

    def get_starting_vertex(self):
        if len(self)==0:
            return None

        if self.__root is None:
            self.set_root()
        return self.__root

    # def get_starting_vertex(self):
    #     vertex = None
    #     for vertex in self.vertices():
    #         keyword_match,alias = vertex
    #         if not keyword_match.is_free():
    #             break
    #     return vertex

        
    def add_vertex(self, vertex):
        results = super().add_vertex(vertex)
        if self.__root is None:
            self.set_root(vertex)
        return results

    def add_keyword_match(self, heyword_match, **kwargs):
        alias = kwargs.get('alias', 't{}'.format(self.__len__()+1))
        vertex = (heyword_match, alias)
        return self.add_vertex(vertex)

    def add_adjacent_keyword_match(self,parent_vertex,keyword_match,edge_direction='>',**kwargs):
        child_vertex = self.add_keyword_match(keyword_match,**kwargs)
        self.add_edge(parent_vertex,child_vertex,edge_direction=edge_direction)

    def keyword_matches(self):
        # return {keyword_match for keyword_match,alias in self.vertices()}
        for keyword_match,alias in self.vertices():
            yield keyword_match

    def non_free_keyword_matches(self):
        # return {keyword_match for keyword_match,alias in self.vertices() if not keyword_match.is_free()}
        for keyword_match,alias in self.vertices():
            if not keyword_match.is_free():
                yield keyword_match

    def num_free_keyword_matches(self):
        i=0
        for keyword_match,alias in self.vertices():
            if keyword_match.is_free():
                i+=1
        return i

    def is_sound(self):
        if len(self) < 3:
            return True

        #check if there is a case A->B<-C, when A.table==C.table
        for vertex,(outgoing_neighbours,incoming_neighbours) in self._Graph__graph_dict.items():
            if len(outgoing_neighbours)>=2:
                outgoing_tables = set()
                for neighbour,alias in outgoing_neighbours:
                    if neighbour.table not in outgoing_tables:
                        outgoing_tables.add(neighbour.table)
                    else:
                        return False

        return True

    def remove_vertex(self,vertex):
        print('vertex:\n{}\n_Graph__graph_dict\n{}'.format(vertex,self._Graph__graph_dict))
        outgoing_neighbours,incoming_neighbours = self._Graph__graph_dict[vertex]
        for neighbour in incoming_neighbours:
            self._Graph__graph_dict[neighbour][0].remove(vertex)
        self._Graph__graph_dict.pop(vertex)

    def is_total(self,query_match):
        return set(self.non_free_keyword_matches())==set(query_match)

    def contains_keyword_free_match_leaf(self):
        for vertex in self.leaves():
            keyword_match,alias = vertex
            if keyword_match.is_free():
                return True
        return False

    def minimal_cover(self,query_match):
        return self.is_total(query_match) and not self.contains_keyword_free_match_leaf()

    def unaliased_edges(self):
        for (keyword_match,alias),(neighbour_keyword_match,neighbour_alias) in self.edges():
            yield (keyword_match,neighbour_keyword_match)

    def calculate_score(self, query_match):
        # self.score = query_match.total_score*len(query_match)/len(self)
        self.score = query_match.total_score/len(self)

    def __eq__(self, other):

        if not isinstance(other,CandidateNetwork):
            return False
        
        self_root_km,self_root_alias  = self.__root
        # if other.get_root() is None:
        #     print(f'OTHER ROOT IS NONE')
        #     print(other)
        other_root_km,other_root_alias= other.get_root()


        other_hash = None

        if self_root_km==other_root_km:
            other_hash = hash(other)
        else:            
            for keyword_match,alias in other.vertices():
                if self_root_km == keyword_match:
                    root = (keyword_match,alias)
                    # print(f'Root: {root}')
                    other_hash = other.hash_from_root(root)
        
        if other_hash is None:
            return False

        # print(f'Equal:{hash(self)==other_hash}\nSelf:\n{self}\nOther:\n{other}\n\n\n')

        return hash(self)==other_hash

    def __hash__(self):
        if len(self)==0:
            return hash(None)
        if self.__root is None:
            self.set_root()
        return self.hash_from_root(self.__root)   


    def hash_from_root(self,root):
        hashable = []

        level = 0       
        visited = set()

        queue = deque()
        queue.append( (level,root) )

        while queue:
            level,vertex = queue.popleft()
            keyword_match,alias = vertex
            children = Counter()
            visited.add(alias)
            
            for adj_vertex in self.neighbours(vertex):
                adj_km,adj_alias = adj_vertex
                if adj_alias not in visited:
                    queue.append( (level+1,adj_vertex) )
                    children[adj_km]+=1
                
            if len(hashable)<level+1:
                hashable.append(set())
            
            hashable[level].add( (keyword_match,frozenset(children.items())) )

        hashcode = hash(tuple(frozenset(items) for items in hashable))
        return hashcode   

    def __repr__(self):
        if len(self)==0:
            return 'EmptyCN'
        print_string = ['\t'*level+direction+str(vertex[0])  for direction,level,vertex in self.leveled_dfs_iter()]
        return '\n'.join(print_string)

    def to_json_serializable(self):
        return [{'keyword_match':keyword_match.to_json_serializable(),
            'alias':alias,
            'outgoing_neighbours':[alias for (km,alias) in outgoing_neighbours],
            'incoming_neighbours':[alias for (km,alias) in incoming_neighbours]}
            for (keyword_match,alias),(outgoing_neighbours,incoming_neighbours) in self._Graph__graph_dict.items()]

    def to_json(self):
        return json.dumps(self.to_json_serializable())

    def from_json_serializable(json_serializable_cn):
        alias_hash ={}
        edges=[]
        for vertex in json_serializable_cn:
            keyword_match = KeywordMatch.from_json_serializable(vertex['keyword_match'])
            alias_hash[vertex['alias']]=keyword_match

            for outgoing_neighbour in vertex['outgoing_neighbours']:
                edges.append( (vertex['alias'],outgoing_neighbour) )

        candidate_network = CandidateNetwork()
        for alias,keyword_match in alias_hash.items():
            candidate_network.add_vertex( (keyword_match,alias) )
        for alias1, alias2 in edges:
            vertex1 = (alias_hash[alias1],alias1)
            vertex2 = (alias_hash[alias2],alias2)
            candidate_network.add_edge(vertex1,vertex2)

        return candidate_network

    def from_json(json_cn):
        return CandidateNetwork.from_json_serializable(json.loads(json_cn))

    def get_sql_from_cn(self,schema_graph,**kwargs):
        rows_limit=kwargs.get('rows_limit',1000)
        show_evaluation_fields=kwargs.get('show_evaluation_fields',False)
        
        hashtables = {} # used for disambiguation

        selected_attributes = set()
        filter_conditions = []
        disambiguation_conditions = []
        selected_tables = []

        tables__search_id = []
        relationships__search_id = []

        for prev_vertex,direction,vertex in self.dfs_pair_iter(root_predecessor=True):
            keyword_match, alias = vertex
            for type_km, table ,attribute,keywords in keyword_match.mappings():
                selected_attributes.add(f'{alias}.{attribute}')
                if type_km == 'v':
                    for keyword in keywords:
                        sql_keyword = keyword.replace('\'','\'\'')
                        condition = f"CAST({alias}.{attribute} AS VARCHAR) ILIKE \'%{sql_keyword}%\'"
                        filter_conditions.append(condition)

            hashtables.setdefault(keyword_match.table,[]).append(alias)

            if show_evaluation_fields:
                tables__search_id.append(f'{alias}.__search_id')

            if prev_vertex is None:
                selected_tables.append(f'{keyword_match.table} {alias}')
            else:
                # After the second table, it starts to use the JOIN syntax
                prev_keyword_match,prev_alias = prev_vertex
                if direction == '>':
                    constraint_keyword_match,constraint_alias = prev_vertex
                    foreign_keyword_match,foreign_alias = vertex
                else:
                    constraint_keyword_match,constraint_alias = vertex
                    foreign_keyword_match,foreign_alias = prev_vertex

                edge_info = schema_graph.get_edge_info(constraint_keyword_match.table,
                                            foreign_keyword_match.table)

                for constraint in edge_info:
                    join_conditions = []
                    for (constraint_column,foreign_column) in edge_info[constraint]:
                        join_conditions.append(
                            f'{constraint_alias}.{constraint_column} = {foreign_alias}.{foreign_column}'
                        )
                    txt_join_conditions = '\n\t\tAND '.join(join_conditions)
                    selected_tables.append(f'JOIN {keyword_match.table} {alias} ON {txt_join_conditions}')
                if show_evaluation_fields:
                    relationships__search_id.append(f'({alias}.__search_id, {prev_alias}.__search_id)')

        for table,aliases in hashtables.items():
            for i in range(len(aliases)):
                for j in range(i+1,len(aliases)):
                    disambiguation_conditions.append(f'{aliases[i]}.ctid <> {aliases[j]}.ctid')

        if len(tables__search_id)>0:
            tables__search_id = [f"({', '.join(tables__search_id)}) AS Tuples"]
        if len(relationships__search_id)>0:
            txt_relationships = ', '.join(relationships__search_id)
            relationships__search_id = [f'({txt_relationships}) AS Relationships']

        sql_text = '\nSELECT\n\t{}\nFROM\n\t{}\nWHERE\n\t{}\nLIMIT {};'.format(
            ',\n\t'.join( tables__search_id+relationships__search_id+list(selected_attributes) ),
            '\n\t'.join(selected_tables),
            '\n\tAND '.join( disambiguation_conditions+filter_conditions),
            rows_limit)
        return sql_text
