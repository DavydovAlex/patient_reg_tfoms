import abc
import copy
import os
import oracledb as ora
from time import sleep
from datetime import datetime, date
import yaml
import pandas as pd
import logging
import time
import csv

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

class OidsToReplace(SubQuery):
    _file = 'OIDS_TO_REPLACE_subquery.sql'
    _name = 'OIDS_TO_REPLACE'
    _vars = []

class SmpOidsToReplace(SubQuery):
    _file = 'SMP_OIDS_TO_REPLACE_subquery.sql'
    _name = 'SMP_OIDS_TO_REPLACE'
    _vars = []

class Faps(SubQuery):
    _file = 'FAPS_subquery.sql'
    _name = 'FAP'
    _vars = []

def read_config(path):
    if not os.path.exists(path):
        raise FileNotFoundError('Файл {} не найден'.format(path))
    with open(path, "r") as file:
        config_data = yaml.load(file, yaml.Loader)
    return config_data


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename="/data/log.log", filemode="w")
    mo_list_all = read_config('MO_LIST_ALL.yml')
    print(mo_list_all)
    mo_list_amb = read_config('MO_LIST_AMBULATORY.yml')
    print(mo_list_amb)
    config = read_config('config.yml')
    if 'DATE_REESTR' in config:
        if config['DATE_REESTR'] is not None:
            date_reestr = datetime.strptime(config['DATE_REESTR'], '%d.%m.%Y').date().strftime('%d.%m.%Y')
        else:
            date_reestr = date.today().strftime('%d.%m.%Y')
    else:
        date_reestr = date.today().strftime('%d.%m.%Y')
    logging.info("DATE_REESTR: {}".format(date_reestr))

    ora.init_oracle_client()
    connection_ = ora.connect(user=MIS_USER,
                              password=MIS_PASSWORD,
                              dsn=ora.makedsn(host=MIS_HOST, port=MIS_PORT, service_name=MIS_SERVICE_NAME))
    params = date_reestr
    cursor = connection_.cursor()
    start_time = time.time()
    cursor.execute(Query.read_sql('sql/ALIVE_AGENTS.sql'), {'DATE_REESTR': date_reestr})
    agents = [agent[0] for agent in cursor.fetchall()]
    logging.info('Количество агентов физлиц составляет: {}. Выполнено за {} c '.
                 format(len(agents), time.time()-start_time))
    agents_split = [agents[i:i + 500000] for i in range(0, len(agents), 500000)]

    cursor.close()
    connection_.close()
    results_list = []
    columns_list = ['REF_ID_PER',
                    'SNAME',
                    'NAME',
                    'MIDDLE_NAM',
                    'DATE_BIRTH',
                    'ENP',
                    'CONTACTS',
                    'REGISTRATION_ADDR',
                    'REGISTRATION_ADDR_CITY',
                    'RESIDENTIAL_ADDR',
                    'RESIDENTIAL_ADDR_CITY',
                    'DATA_REEST',
                    'REF_ID_HOS',
                    # 'IS_REPLACED_AMB',
                    'DATA_ATTAC',
                    'NOTES',
                    'FID_PERSON',
                    'PRIB_ID',
                    'SP_MO',
                    # 'IS_EMPTY_SP_MO',
                    'PODR',
                    'REF_ID_DEN',
                    'DATA_DENT',
                    'NOTES_DENT',
                    'FID_DENT_S',
                    'SP_MO_DENT',
                    'REF_ID_GIN',
                    'DATA_GINE',
                    'NOTES_GIN',
                    'FID_GINE_S',
                    'SP_MO_GINE',
                    'SMP_OID',
                    'FAP_OID'
                    ]
    file_number = 0
    for agents in agents_split:
        connection_ = ora.connect(user=MIS_USER,
                                  password=MIS_PASSWORD,
                                  dsn=ora.makedsn(host=MIS_HOST, port=MIS_PORT, service_name=MIS_SERVICE_NAME))
        cursor = connection_.cursor()
        start_time = time.time()
        sql = SubQuery.join(
            LpuList.make_subquery(ids=read_config('MO_LIST_ALL.yml')),
            LpuAmbList.make_subquery(ids=read_config('MO_LIST_AMBULATORY.yml')),
            Oids.make_subquery(),
            Faps.make_subquery(),
            OidsToReplace.make_subquery(),
            SmpOidsToReplace.make_subquery(),
            Agents.make_subquery(enp_prefix='`', agents=agents),
            AgentReqistration.make_subquery()
        )
        sql += '\n' + Query.read_sql('sql/CORE.sql')
        cursor.execute(sql, {'DATE_REESTR': date_reestr})
        result = cursor.fetchall()
        logging.info('Добавлено {} пациентов. Выполнено за {} c '.format(len(result), time.time()-start_time))
        results_list += result
        if len(results_list) + 500000 > 999999:
            filename_pattern = 'ATTACH_'
            extension = '.csv'
            rows_to_write = [list(row) for row in results_list]
            with open('/data/' + filename_pattern + str(file_number) + extension, mode='w', newline='',
                      encoding='utf-8-sig') as file:
                csv_writer = csv.writer(file, delimiter=';', quoting=csv.QUOTE_ALL)
                csv_writer.writerow(columns_list)
                csv_writer.writerows(rows_to_write)
                logging.info('{} записано в файл {}'.format(len(rows_to_write), filename_pattern + str(file_number) + extension))
                file_number += 1
                results_list = []
        cursor.close()
        connection_.close()
    logging.info("Общее число записей: {}".format(len(results_list)))
    filename_pattern = 'ATTACH_'
    extension = '.csv'
    rows_to_write = [list(row) for row in results_list]
    with open('/data/' + filename_pattern + str(file_number) + extension, mode='w', newline='',
              encoding='utf-8-sig') as file:
        csv_writer = csv.writer(file, delimiter=';', quoting=csv.QUOTE_ALL)
        csv_writer.writerow(columns_list)
        csv_writer.writerows(rows_to_write)
        logging.info(
            '{} записано в файл {}'.format(len(rows_to_write), filename_pattern + str(file_number) + extension))
    #
    # rows_to_write = [list(row) for row in results_list]
    # rows_to_write_split = [rows_to_write[i:i + 999999] for i in range(0, len(rows_to_write), 999999)]
    #
    # for i, chunck in enumerate(rows_to_write_split):
    #     with open('/data/' + filename_pattern + str(i) + extension, mode='w', newline='',
    #               encoding='utf-8-sig') as file:
    #         csv_writer = csv.writer(file, delimiter=';', quoting=csv.QUOTE_ALL)
    #         csv_writer.writerow(columns_list)
    #         csv_writer.writerows(chunck)
    #         logging.info('{} записано в файл {}'.format(len(chunck), filename_pattern + str(i) + extension))

    logging.info("Выгруженные файлы расположены в /data/")

