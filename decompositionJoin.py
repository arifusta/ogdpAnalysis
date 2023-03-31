import pickle
from pymongo import MongoClient
from multiprocessing import Pool
from dataclasses import dataclass
from tqdm import tqdm
from normalizationUtil import *
from collections import Counter
import ijson
import uuid
import duckdb

@dataclass
class decomposedPair:
    leftTableID: str
    leftColumnIndex: int
    rightTableID : str
    rightColumnIndex: int
    leftDecomposedID : int = -1
    rightDecomposedID : int = -1
    singleSideDecomposed : bool = False
    isOptimal: bool = False
    leftDecompUniqueScore : float = 0
    rightDecompUniqueScore : float = 0
    decompCard : str = ''
    decompJoinCount: int = 0
    decompExpRatio: float = 0


# def compare_columns(pair, leftAttributes, rightAttributes):
#     duckdb.query("PRAGMA threads=2;")
#     view_name = "view_" + str(uuid.uuid1().hex)
#
#     try:
#         table_1 = df_dict[pair['leftTableID']].loc[:, leftAttributes]
#         col_names = [str(i) for i in range(len(table_1.columns))]
#         table_1.columns = col_names
#
#         table_2 = df_dict[pair['rightTableID']].loc[:, rightAttributes]
#         col_names = [str(i) for i in range(len(table_2.columns))]
#         table_2.columns = col_names
#
#         query_string = 'CREATE VIEW %s AS\
#             (\
#                 WITH T1 AS (SELECT "%d" AS col_1, COUNT(*) AS count_1 FROM table_1 GROUP BY "%d"),\
#                     T2 AS (SELECT "%d" AS col_2, COUNT(*) AS count_2 FROM table_2 GROUP BY "%d")\
#                     SELECT *, (count_1 * count_2) AS total_count FROM T1 INNER JOIN  T2 ON T1.col_1 = T2.col_2\
#             )' % (
#         view_name, pair['leftColumnIndex'], pair['leftColumnIndex'], pair['rightColumnIndex'], pair['rightColumnIndex'])
#
#         duckdb.query(query_string)
#         joined_count_result = duckdb.query('SELECT SUM(total_count) AS aggregated FROM %s' % view_name).to_df()
#         joined_count = int(joined_count_result['aggregated'][0])
#
#         cardinality = "one:one"
#         is_many_many_result = duckdb.query(
#             'SELECT * FROM %s WHERE count_1 > 1 AND count_2 > 1 LIMIT 1' % view_name).to_df()
#         if len(is_many_many_result) > 0:
#             cardinality = "many:many"
#         else:
#             is_many_one_result = duckdb.query(
#                 'SELECT * FROM %s WHERE (count_1 > 1 AND count_2 = 1) OR (count_1 = 1 AND count_2 > 1) LIMIT 1' % view_name).to_df()
#             if len(is_many_one_result) > 0:
#                 cardinality = "one:many"
#         duckdb.query('DROP VIEW IF EXISTS %s' % view_name)
#         return {
#             'pair': pair,
#             'cardinality': cardinality,
#             'joined_count': joined_count
#         }
#     except Exception as e:
#         # print(e)
#         return {
#             'pair': pair
#         }

def getRelatedPairs(normTableDicts, tableColumnDicts):
    print(f'Reading the giant json file including joinable pairs.')
    file = 'results_perfect_jaccard.json'
    perfectPairs = []

    with open(file) as f:
        for record in tqdm(ijson.items(f, "item")):
            if record['pair'][0]['uuid'] in normTableDicts and record['pair'][1]['uuid'] in normTableDicts and \
                    record['pair'][0]['index'] in tableColumnDicts[1][record['pair'][0]['uuid']] and record['pair'][1][
                'index'] in tableColumnDicts[1][record['pair'][1]['uuid']]:
                perfectPairs.append({'leftTableID': record['pair'][0]['uuid'],
                                     'leftColumnIndex': record['pair'][0]['index'],
                                     'rightTableID': record['pair'][1]['uuid'],
                                     'rightColumnIndex': record['pair'][1]['index'],
                                     'orgCard': record['cardinality'],
                                     'orgJoinCount': record['joined_count']

                                     })
    print(len(perfectPairs))
    return perfectPairs


def compare_columns(pair):
    import duckdb
    duckdb.query("PRAGMA threads=2;")
    view_name = "view_" + str(uuid.uuid1().hex)
    portal = pair['portal']

    #pair = j['pair']
    try:
        if portal == 'SG':
            table_1 = readCSVFromZipSG(pair['leftTableDoc'])
            table_1.columns = list(range(0, table_1.shape[1]))
            table_2 = readCSVFromZipSG(pair['rightTableDoc'])
            table_2.columns = list(range(0, table_2.shape[1]))
        elif portal == 'CA':
            table_1 = readCSVFromZipCA(pair['leftTableDoc'])
            table_1.columns = list(range(0, table_1.shape[1]))
            table_2 = readCSVFromZipCA(pair['rightTableDoc'])
            table_2.columns = list(range(0, table_2.shape[1]))
        elif portal == 'UK':
            table_1 = readCSVFromZipUK(pair['leftTableDoc'])
            table_1.columns = list(range(0, table_1.shape[1]))
            table_2 = readCSVFromZipUK(pair['rightTableDoc'])
            table_2.columns = list(range(0, table_2.shape[1]))
        elif portal == 'US':
            table_1 = readCSVFromZipUS(pair['leftTableDoc'])
            table_1.columns = list(range(0, table_1.shape[1]))
            table_2 = readCSVFromZipUS(pair['rightTableDoc'])
            table_2.columns = list(range(0, table_2.shape[1]))

        query_string = 'CREATE VIEW %s AS\
            (\
                WITH T1 AS (SELECT "%d" AS col_1, COUNT(*) AS count_1 FROM table_1 GROUP BY "%d"),\
                    T2 AS (SELECT "%d" AS col_2, COUNT(*) AS count_2 FROM table_2 GROUP BY "%d")\
                    SELECT *, (count_1 * count_2) AS total_count FROM T1 INNER JOIN  T2 ON T1.col_1 = T2.col_2\
            )' % (view_name, pair['leftColumnIndex'], pair['leftColumnIndex'], pair['rightColumnIndex'], pair['rightColumnIndex'])

        duckdb.query(query_string)
        joined_count_result = duckdb.query('SELECT SUM(total_count) AS aggregated FROM %s' % view_name).to_df()
        joined_count = int(joined_count_result['aggregated'][0])

        cardinality = "one:one"
        is_many_many_result = duckdb.query(
            'SELECT * FROM %s WHERE count_1 > 1 AND count_2 > 1 LIMIT 1' % view_name).to_df()
        if len(is_many_many_result) > 0:
            cardinality = "many:many"
        else:
            is_many_one_result = duckdb.query(
                'SELECT * FROM %s WHERE (count_1 > 1 AND count_2 = 1) OR (count_1 = 1 AND count_2 > 1) LIMIT 1' % view_name).to_df()
            if len(is_many_one_result) > 0:
                cardinality = "many:one"
        duckdb.query('DROP VIEW IF EXISTS %s' % view_name)
        return {
            'pair': pair,
            'score' : pair['score'],
            'cardinality': cardinality,
            'joined_count': joined_count,
            'expRatio' : joined_count / max(table_1.shape[0], table_2.shape[0])
        }
    except Exception as e:
        print(e)
        return {
            'pair': pair
        }