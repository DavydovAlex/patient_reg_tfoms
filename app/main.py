import abc
import copy
import os
import oracledb as ora
from time import sleep
from datetime import datetime, date
import yaml
import pandas as pd


MIS_HOST = os.environ.get('MIS_HOST')
MIS_PORT = int(os.environ.get('MIS_PORT'))
MIS_SERVICE_NAME = os.environ.get('MIS_SERVICE_NAME')
MIS_USER = os.environ.get('MIS_USER')
MIS_PASSWORD = os.environ.get('MIS_PASSWORD')


class FormatVars(abc.ABC):

    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    @abc.abstractmethod
    def prepare(self, var):
        pass


class ListVar(FormatVars):

    @classmethod
    def _transform(cls, var, element_handler):
        result = []
        splitted_list = [var[i:i + 999] for i in range(0, len(var), 999)]
        for elem in splitted_list:
            result.append(','.join([element_handler(item) for item in elem]))
        if len(result) == 1:
            return result[0]
        else:
            return result


class StringVar(FormatVars):
    def prepare(self, var):
        return "'" + str(var) + "'" if var is not None else "''"


class ListIntVar(ListVar):
    def prepare(self, var):
        return self._transform(var, lambda val: str(val))

class ListStrVar(ListVar):

    def prepare(self, var):
        return self._transform(var, lambda val: "'" + str(val) + "'")



class Query:

    @staticmethod
    def read_sql(path) -> str:
        with open(path, "r") as file:
            return file.read()

    @staticmethod
    def union(*queries):
        return '\nUNION ALL\n'.join(queries)

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



def read_config(path):
    if not os.path.exists(path):
        raise FileNotFoundError('Файл {} не найден'.format(path))
    with open(path, "r") as file:
        config_data = yaml.load(file, yaml.Loader)
    return config_data


if __name__ == '__main__':
    mo_list_all = read_config('MO_LIST_ALL.yml')
    mo_list_amb = read_config('MO_LIST_AMBULATORY.yml')
    config = read_config('config.yml')
    if 'DATE_REESTR' in config:
        if config['DATE_REESTR'] is not None:
            date_reestr = datetime.strptime(config['DATE_REESTR'], '%d.%m.%Y').date().strftime('%d.%m.%Y')
        else:
            date_reestr = date.today().strftime('%d.%m.%Y')
    else:
        date_reestr = date.today().strftime('%d.%m.%Y')

    print(date_reestr)

    rng = [i for i in range(1300)]
    {'enp_prefix':'`','agents':rng}
    print(Agents.make_subquery(**{'enp_prefix': '`', 'agents': rng}))


    # ora.init_oracle_client()
    # connection_ = ora.connect(user=MIS_USER,
    #                           password=MIS_PASSWORD,
    #                           dsn=ora.makedsn(host=MIS_HOST, port=MIS_PORT, service_name=MIS_SERVICE_NAME))
    # sql = get_agents()
    # params = date_reestr
    # cursor = connection_.cursor()
    # cursor.execute(sql, {'DATE_REESTR': date_reestr})
    # cursor.prepare()
    # agents = cursor.fetchall()
    # count = 0
    # agents_subquery = "SEL"
    # for agent_row in agents:
    #     agent_id = agent_row[0]
    #     if count == 999:


