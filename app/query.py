import dataclasses
import os
import abc
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def read(path:[str,Path]) -> str:
    with open(path, "r") as file:
        return file.read()

class BindVar(abc.ABC):
    _name: str
    _value: Any

    def __init__(self, name: str, value: Any):
        self._name = name
        self._value = value

    @property
    def name(self):
        return self._name

    @property
    @abc.abstractmethod
    def value(self):
        pass


    def bind(self, sql: str):
        if self.name in sql or self.name.lower() in sql:
            sql_new = sql.replace(':{}'.format(self.name),)
            return sql.replace(':{}'.format)


class ListVar(abc.ABC, BindVar):

    @property
    @abc.abstractmethod
    def value(self):
        pass

    def _transform(self, value, handler):
        if isinstance(value, list):
            return [handler(item) for item in value]
        else:
            raise Exception('"value" must be a list')



class ListIntVar(ListVar):

    @property
    def value(self):
        return self._transform(self._value, lambda val: str(val))


class ListStrVar(ListVar):
    @property
    def value(self):
        return self._transform(self._value, lambda val: "'" + str(val) + "'")


class StringVar(BindVar):
    @property
    def value(self):
        return "'" + str(self._value) + "'" if self._value is not None else "''"




@dataclass
class _QueryObject:
    query: str
    vars_: dict

@dataclass
class Query(_QueryObject):
    columns: list[str]

@dataclass
class SubQuery(_QueryObject):
    name: str


class Builder:

    # @dataclasses.dataclass
    # class PreparedQuery:
    #     sql: str
    #     columns: list[str]

    @staticmethod
    def build_query(*queries: _QueryObject):
        processed_queries = []
        for query in queries:
            if isinstance(query, SubQuery):
                processed_queries.append(Builder.__process_subquery(query))
            elif isinstance(query, Query):
                processed_queries.append(Builder.__process_query(query))
        if len(processed_queries) > 1:


    @staticmethod
    def __process_subquery(query:_QueryObject):

        raw_sql = cls.read_sql(os.path.join(cls.query_dir, cls._file))
        prepared_vars = cls._prepare(**vars)
        if len(prepared_vars) == 0:
            return raw_sql
        single_vars = dict()
        list_vars = dict()
        for var, value in prepared_vars.items():
            if isinstance(value, list):
                list_vars[var] = value
            else:
                single_vars[var] = value
        if len(list_vars) == 0:
            return raw_sql.format(**single_vars)
        elif len(list_vars) == 1:
            queries = []
            key = list(list_vars.keys())[0]
            value = list(list_vars.values())[0]
            for split in value:
                vars_ = copy.copy(single_vars)
                vars_[key] = split
                queries.append(raw_sql.format(**vars_))
            return cls.union(*queries)
        elif len(list_vars) > 1:
            raise Exception('Не реализована возможность генерации запроса более чем с одной переменной типа список ')

    @staticmethod
    def replace

    @staticmethod
    def __process_query(query:Query):
        pass



class SubQuery(Query):
    query_dir: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql')
    _file: str = None
    _name: str = None
    _vars: list[FormatVars] = None

    @classmethod
    def _get_var(cls, name) -> FormatVars:
        for var in cls._vars:
            if var.name == name:
                return var
        raise Exception('Переменная {} не найдена'.format(name))

    @staticmethod
    def join(*subqueries):
        return 'WITH ' + ',\n'.join(subqueries)



    @classmethod
    def _prepare(cls, **vars_) -> dict:
        prepared_vars = dict()
        if len(vars_) == 0:
            return dict()
        for var_name, value in vars_.items():
            prepared_vars[var_name] = cls._get_var(var_name).prepare(value)
        return prepared_vars

    @classmethod
    def make_sql(cls, **vars):
        raw_sql = cls.read_sql(os.path.join(cls.query_dir, cls._file))
        prepared_vars = cls._prepare(**vars)
        if len(prepared_vars) == 0:
            return raw_sql
        single_vars = dict()
        list_vars = dict()
        for var, value in prepared_vars.items():
            if isinstance(value, list):
                list_vars[var] = value
            else:
                single_vars[var] = value
        if len(list_vars) == 0:
            return raw_sql.format(**single_vars)
        elif len(list_vars) == 1:
            queries = []
            key = list(list_vars.keys())[0]
            value = list(list_vars.values())[0]
            for split in value:
                vars_ = copy.copy(single_vars)
                vars_[key] = split
                queries.append(raw_sql.format(**vars_))
            return cls.union(*queries)
        elif len(list_vars) > 1:
            raise Exception('Не реализована возможность генерации запроса более чем с одной переменной типа список ')

    @classmethod
    def make_subquery(cls, **vars):
        return '{} AS(\n'.format(cls._name) + cls.make_sql(**vars) + ')'



class Agents(SubQuery):
    _file = 'AGENTS_subquery.sql'
    _name = 'AGENTS'
    _vars = [StringVar('enp_prefix'),
             ListIntVar('agents')]


class LpuList(SubQuery):
    _file = 'MO_LIST_subquery.sql'
    _name = 'MO_LIST'
    _vars = [ListIntVar('ids')]


class LpuAmbList(SubQuery):
    _file = 'MO_LIST_subquery.sql'
    _name = 'MO_LIST_AMB'
    _vars = [ListIntVar('ids')]

class AgentReqistration(SubQuery):
    _file = 'AGENT_REGISTRATION_subquery.sql'
    _name = 'AGENT_REGISTRATION'
    _vars = []

class Oids(SubQuery):
    _file = 'OIDS_subquery.sql'
    _name = 'OIDS'
    _vars = []