import argparse
from datetime import datetime, timedelta

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import GroupByKey

from base_classes import DoFnBase
from postgres_db import PostgresDb


# requirements:
# beam/sdks/python/container/base_image_requirements.txt
# +
# beam/sdks/python/setup.py


def collection_range_timestamps(startDate, endDate,
                                delta=timedelta(minutes=30),
                                return_as_list=False):
    times = []

    current_date = startDate

    while current_date < endDate:
        date_from = (current_date).strftime("%Y-%m-%d %H:%M:%S")
        date_to = (current_date + delta).strftime("%Y-%m-%d %H:%M:%S")
        times.append(tuple([date_from, date_to]))
        current_date += delta

    return times if return_as_list else beam.Create(times)


'''
def collection_dumped_files(file_storage):
    blob_names = []

    blob_prefixes = ['stat_source']

    for blob in file_storage.list_blobs():
        blob_name = blob.name
        for prefix in blob_prefixes:
            if prefix in blob_name:
                if blob_name not in blob_names:
                    blob_names.append(blob_name)
                break

    return beam.Create(blob_names)
'''


def collection_heroes():
    import requests

    heroes = []

    result = requests.get('https://raw.githubusercontent.com/odota/dotaconstants/master/build/heroes.json')
    result_json = result.json()

    for num, hero_data in result_json.items():
        hero_id = hero_data.get('id', 0)

        if hero_id > 0:
            heroes.append(hero_id)

    return beam.Create(heroes)


class DoFnQueryPurchLog(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnQueryPurchLog, self).__init__(**kwargs)

    def process(self, boundaries_tuple):
        queries = []

        boundaries_tuple_len = len(boundaries_tuple)

        start = None if boundaries_tuple_len != 2 else boundaries_tuple[0]
        end = None if boundaries_tuple_len != 2 else boundaries_tuple[1]

        if start and end:
            queries.append({'start_date': start,
                            'end_date': end,
                            'sql': 'select match_id, '
                                   'player_num, '
                                   'time_tick, '
                                   'key '
                                   'from ods.matches_players_purchase_log '
                                   'where match_start_time >= timestamp \'{0}\' '
                                   'and match_start_time < timestamp \'{1}\' '.format(
                                start, end)})

        return queries


class DoFnQueryMatchesPlayers(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnQueryMatchesPlayers, self).__init__(**kwargs)

    def process(self, boundaries_tuple):
        queries = []

        boundaries_tuple_len = len(boundaries_tuple)

        start = None if boundaries_tuple_len != 2 else boundaries_tuple[0]
        end = None if boundaries_tuple_len != 2 else boundaries_tuple[1]

        if start is not None and end is not None:
            queries.append({'start_date': start,
                            'end_date': end,
                            'sql': 'select match_id, '
                                   'player_num, '
                                   'hero_id, '
                                   'coalesce(account_id, 0) as account_id, '
                                   'lane, '
                                   'coalesce(rank_tier, 0) as rank_tier, '
                                   'duration, '
                                   'win, '
                                   'enemies_array[1] as enemy_1, '
                                   'enemies_array[2] as enemy_2, '
                                   'enemies_array[3] as enemy_3, '
                                   'enemies_array[4] as enemy_4, '
                                   'enemies_array[5] as enemy_5 '
                                   'from ods.matches_players '
                                   'where match_start_time >= timestamp \'{0}\' '
                                   'and match_start_time < timestamp \'{1}\' '.format(
                                start, end)})

        return queries


class DoFnExecuteSql(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnExecuteSql, self).__init__(**kwargs)

    def process(self, query_data):
        sql = query_data['sql']

        tuples = []
        if self.pg_db is not None:
            rows = self.pg_db.select_pure(sql)
            if rows:
                tuples = [('{0}_{1}'.format(r[0], r[1]), r) for r in rows]

        #tuples.sort(key=lambda tup: tup[0])

        return tuples



class DoFnRetrieveHeroesItemsPurchTime(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnRetrieveHeroesItemsPurchTime, self).__init__(**kwargs)

    def process(self, merged_tuple):
        result_tuples = []

        (key, merged_data) = merged_tuple

        purch_log_data = merged_data.get('purch_log')
        matches_players_data = merged_data.get('matches_players')

        if isinstance(purch_log_data, list) and isinstance(matches_players_data, list):
            if len(purch_log_data) > 0 and len(matches_players_data) > 0:
                (match_id,
                 player_num,
                 hero_id,
                 account_id,
                 lane,
                 rank_tier,
                 duration,
                 win,
                 enemy1_id,
                 enemy2_id,
                 enemy3_id,
                 enemy4_id,
                 enemy5_id) = matches_players_data[0]

                for row_purch_log in purch_log_data:
                    (_, __, time_tick, item) = row_purch_log

                    key = '{0}_{1}'.format(hero_id, lane)
                    value = tuple([hero_id,
                                   lane,
                                   item,
                                   time_tick,
                                   match_id,
                                   win,
                                   enemy1_id,
                                   enemy2_id,
                                   enemy3_id,
                                   enemy4_id,
                                   enemy5_id])

                    result_tuples.append((key, value))

        return result_tuples


class DoFnDumpStatSource(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnDumpStatSource, self).__init__(**kwargs)

    def process(self, hero_tuple):
        import pandas as pd

        (key, hero_data) = hero_tuple

        df = pd.DataFrame(hero_data, columns=['hero_id',
                                              'lane',
                                              'item',
                                              'time_tick',
                                              'match_id',
                                              'win',
                                              'enemy1_id',
                                              'enemy2_id',
                                              'enemy3_id',
                                              'enemy4_id',
                                              'enemy5_id'])

        self.pg_db.insert_into_table_with_df(table_name='stage.tmp_items_mean_purchase_time',
                                             df=df,
                                             close_connection=True)

        return [1]


class DoFnCalculatePurchStatistics(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnCalculatePurchStatistics, self).__init__(**kwargs)

    def process(self, hero_id):
        import pandas as pd
        import numpy as np

        items_mean_purch_time = []
        items_enemy_win = {}
        items_win = {}

        for lane in range(1,6):
            select_stm = 'select item, ' \
                         'time_tick, ' \
                         'match_id, ' \
                         'win, ' \
                         'enemy1_id, ' \
                         'enemy2_id, ' \
                         'enemy3_id, ' \
                         'enemy4_id, ' \
                         'enemy5_id ' \
                         'from stage.tmp_items_mean_purchase_time ' \
                         'where hero_id={0} and lane={1}'.format(hero_id, lane)

            rows = self.pg_db.select_pure(select_stm)

            df = pd.DataFrame(rows, columns=['item',
                                            'time_tick',
                                            'match_id',
                                            'win',
                                            'enemy1_id',
                                            'enemy2_id',
                                            'enemy3_id',
                                            'enemy4_id',
                                            'enemy5_id'
                                            ])

            df_array = df.values

            if df_array.shape[0] < 1:
                continue

            unique_items = np.unique(df_array[:, df.columns.get_loc('item')])

            for item in unique_items:
                item_arr = df_array[(df_array[:, df.columns.get_loc('item')] == item)]
                unique_matches = np.unique(item_arr[:, df.columns.get_loc('match_id')])

                matches_count = len(unique_matches)
                time_tick_arr = item_arr[:, df.columns.get_loc('time_tick')].astype(int)

                if matches_count > 0:
                    median_time_tick = np.nan_to_num(np.percentile(time_tick_arr, 50))
                    items_mean_purch_time.append(np.array([hero_id, lane, item, median_time_tick, matches_count]))

                    item_data = items_win.get(item)
                    if not item_data:
                        item_data = {'wins': 0, 'matches_count': 0}

                    item_data['matches_count'] += matches_count

                    for match_id in unique_matches:
                        match_arr = item_arr[(item_arr[:, df.columns.get_loc('match_id')] == match_id)][:1]
                        if match_arr.shape[0] > 0:
                            row = match_arr[0]
                            win = row[df.columns.get_loc('win')]
                            item_data['wins'] += win

                            for eid in range(1, 6):
                                enemy_id = row[df.columns.get_loc('enemy{0}_id'.format(eid))]

                                item_enemy_data = items_enemy_win.get(item)
                                if not item_enemy_data:
                                    item_enemy_data = {}

                                enemy_data = item_enemy_data.get(enemy_id)
                                if not enemy_data:
                                    enemy_data = {'wins': 0, 'matches_count': 0}

                                enemy_data['wins'] += win
                                enemy_data['matches_count'] += 1

                                item_enemy_data[enemy_id] = enemy_data
                                items_enemy_win[item] = item_enemy_data

                    items_win[item] = item_data

                df_array = df_array[(df_array[:, df.columns.get_loc('item')] != item)]

        if items_mean_purch_time:
            mean_df = pd.DataFrame(items_mean_purch_time,
                                   columns=['hero_id',
                                            'lane',
                                            'item',
                                            'purch_time',
                                            'matches_count'])

            self.update_by_delete_insert_with_df(
                table_name='datamart.items_mean_purchase_time',
                df=mean_df,
                key_column_name='hero_id',
                key_column_type=int)

        items_enemies_winrate = []
        if items_enemy_win.keys():
            for item, item_data in items_enemy_win.items():
                for enemy_id, enemy_data in item_data.items():
                    enemy_wins = enemy_data['wins']
                    enemy_matches_count = enemy_data['matches_count']

                    items_enemies_winrate.append(np.array([hero_id,
                                                           item,
                                                           enemy_id,
                                                           enemy_wins,
                                                           enemy_matches_count]))

        if items_enemies_winrate:
            enemies_df = pd.DataFrame(items_enemies_winrate,
                                       columns=['hero_id',
                                                'item',
                                                'enemy_id',
                                                'wins',
                                                'matches_count'])

            self.update_by_delete_insert_with_df(
                table_name='datamart.items_hero_enemy_winrate',
                df=enemies_df,
                key_column_name='hero_id',
                key_column_type=int)


        items_winrate = []
        if items_win.keys():
            for item, item_data in items_win.items():
                item_wins = item_data['wins']
                item_matches_count = item_data['matches_count']

                items_winrate.append(
                    np.array([hero_id, item, item_wins, item_matches_count]))

        if items_winrate:
            items_df = pd.DataFrame(items_winrate,
                                    columns=['hero_id',
                                             'item',
                                             'wins',
                                             'matches_count'])

            self.update_by_delete_insert_with_df(
                table_name='datamart.items_hero_winrate',
                df=items_df,
                key_column_name='hero_id',
                key_column_type=int)

        return [1]


'''
class DoFnDropDumpedFiles(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnDropDumpedFiles, self).__init__(**kwargs)

    def process(self, blob_name):
        if self.file_storage is not None:
            self.file_storage.delete_blob(blob_name)

        return [1]
'''

def list_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def parse_string_date(string_date):
    return datetime.strptime(string_date, '%Y-%m-%d %H:%M:%S')


def run(argv=None):
    # TODO: DROP indexes on purch log in DB

    parser = argparse.ArgumentParser()
    parser.add_argument('--startDate',
                        dest='startDate',
                        type=parse_string_date,
                        default=(datetime.now() - timedelta(days=30)).replace(
                            hour=0,
                            minute=0,
                            second=0,
                            microsecond=0),
                        help='Start date.')
    parser.add_argument('--endDate',
                        dest='endDate',
                        type=parse_string_date,
                        default=datetime.now().replace(
                            hour=23,
                            minute=59,
                            second=59,
                            microsecond=999999),
                        help='End date.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pg = PostgresDb()
    pipeline_options = PipelineOptions(pipeline_args)


    # clean dumped files
    pg.truncate_table_by_delete(table_name='stage.tmp_items_mean_purchase_time')

    time_boundaries_list = collection_range_timestamps(
        startDate=known_args.startDate,
        endDate=known_args.endDate,
        delta=timedelta(hours=1),
        return_as_list=True)

    #time_boundaries_list = collection_range_timestamps(startDate=datetime(2019, 1, 29, 0, 0, 0),
    #                                                   endDate=datetime(2019, 1, 29, 1, 0, 0),
    #                                                   delta=timedelta(hours=1),
    #                                                   return_as_list=True)

    # perform source data
    for time_boundaries_bulk_list in list_chunks(time_boundaries_list, 15):
        with beam.Pipeline(options=pipeline_options) as p:
            t_boundaries_sources = (p
                                    | 'next_time_boundaries_bulk' >> beam.Create(time_boundaries_bulk_list))

            purch_log_data = (t_boundaries_sources
                              | 'sql_prepare_purchase_log' >> beam.ParDo(DoFnQueryPurchLog())
                              | 'sql_execute_purchase_log' >> beam.ParDo(DoFnExecuteSql(table_tag='purchase_log', pg_db=pg))
            )

            matches_players_data = (t_boundaries_sources
                                    | 'sql_prepare_matches_players' >> beam.ParDo(DoFnQueryMatchesPlayers())
                                    | 'sql_execute_matches_players' >> beam.ParDo(DoFnExecuteSql(table_tag='matches_players', pg_db=pg))
            )

            ({'purch_log': purch_log_data, 'matches_players': matches_players_data}
            | 'group_by_match_id_player_num' >> beam.CoGroupByKey()
            | 'retrieve_heroes_items_purch_times' >> beam.ParDo(DoFnRetrieveHeroesItemsPurchTime())
            | 'group_by_heroes' >> GroupByKey()
            | 'dump_stat_source' >> beam.ParDo(DoFnDumpStatSource(pg_db=pg))
            )

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'heroes_collection' >> collection_heroes()
         | 'calculate_purch_statistics' >> beam.ParDo(DoFnCalculatePurchStatistics(pg_db=pg)))


if __name__ == '__main__':
    run()
