import sys
import argparse

from datetime import date, datetime, timedelta

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import GroupByKey

from base_classes import DoFnBase
#from file_storage import FileStorage
from postgres_db import PostgresDb


# requirements:
#beam/sdks/python/container/base_image_requirements.txt
#+
#beam/sdks/python/setup.py


def collection_range_timestamps(startDate, endDate, delta=timedelta(minutes=30), return_as_list=False):
    times = []

    current_date = startDate

    while current_date < endDate:
        date_from = (current_date).strftime("%Y-%m-%d %H:%M:%S")
        date_to = (current_date + delta).strftime("%Y-%m-%d %H:%M:%S")
        times.append(tuple([date_from, date_to]))
        current_date += delta

    return times if return_as_list else beam.Create(times)


def collection_patches(pg_db):
    patches = []

    sql_max_patch1 = 'select max(patch) as patch ' \
                     'from ods.matches_heroes_impact'
    if pg_db:
        rows1 = pg_db.select_pure(sql_max_patch1)
        if rows1:
            patch_1 = rows1[0][0]
            patches.append(patch_1)

            '''
            sql_max_patch2 = 'select max(patch) as patch ' \
                             'from ods.matches_heroes_impact ' \
                             'where patch < {0}'.format(patch_1)

            rows2 = pg_db.select_pure(sql_max_patch2)

            if rows2:
                patch_2 = rows2[0][0]
                patches.append(patch_2)
            '''

    return beam.Create(patches)


class DoFnBuidQueryMatchesPlayers(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnBuidQueryMatchesPlayers, self).__init__(**kwargs)

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
                                    'hero_id, '
                                    'coalesce(account_id, 0) as account_id, '
                                    'lane, '
                                    'duration, '
                                    'enemies_array[1] as enemy_1, '
                                    'enemies_array[2] as enemy_2, '
                                    'enemies_array[3] as enemy_3, '
                                    'enemies_array[4] as enemy_4, '
                                    'enemies_array[5] as enemy_5, '
                                    'gold_t[9] as gold_t_10, '
                                    'actions_per_min, '
                                    'kills, ' 
                                    'hero_damage, '
                                    'tower_damage_raw as tower_damage, '
                                    'stuns, '
                                    'deaths, '
                                    'hero_healing, '
                                    'observer_uses, '
                                    'assists, '
                                    'win, '
                                    'coalesce(rank_tier, 0) as rank_tier, '
                                    'patch, '
                                    'teamfight_participation '
                                    'from ods.matches_players '
                                    'where match_start_time >= timestamp \'{0}\' ' 
                                    'and match_start_time < timestamp \'{1}\' '.format(start, end)})

        return queries


class DoFnExecuteSql(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnExecuteSql, self).__init__(**kwargs)

    def process(self, query_data):
        table_tag = self.get_argument('table_tag', '')

        sql = query_data['sql']

        tuples = []
        if self.pg_db:
            rows = self.pg_db.select_pure(sql)
            if rows:
                if table_tag == 'purchase_log':
                    tuples = [('{0}_{1}'.format(r[0], r[1]), r) for r in rows]
                else:
                    tuples = [(r[0], r) for r in rows]

        tuples.sort(key=lambda tup: tup[0])

        return tuples


class DoFnEnrichSplitByHeroes(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnEnrichSplitByHeroes, self).__init__(**kwargs)

        self.input_col_idx = {'match_id': 0,
                              'player_num': 1,
                              'hero_id': 2,
                              'account_id': 3,
                              'lane': 4,
                              'duration': 5,
                              'enemy_1': 6,
                              'enemy_2': 7,
                              'enemy_3': 8,
                              'enemy_4': 9,
                              'enemy_5': 10,
                              'gold_t_10': 11,
                              'actions_per_min': 12,
                              'kills': 13,
                              'hero_damage': 14,
                              'tower_damage': 15,
                              'stuns': 16,
                              'deaths': 17,
                              'hero_healing': 18,
                              'observer_uses': 19,
                              'assists': 20,
                              'win': 21,
                              'rank_tier': 22,
                              'patch': 23,
                              'teamfight_participation': 24}

        self.impact_features = {'teamfight_participation': 1,
                                'kills': 1,
                                'assists': 1,
                                'actions_per_min': 1,
                                'hero_damage': 1,
                                'tower_damage': 1,
                                'stuns': 1,
                                'deaths': -1,
                                'hero_healing': 1,
                                'observer_uses': 1}

    def correct_negative_feature(self, axis_array):
        import numpy as np
        return np.power(axis_array + 1, -1)

    def process(self, match_tuple):
        import numpy as np

        heroes_tuples = []

        (match_id, match_data) = match_tuple
        match_array = np.array(match_data).astype(float)

        output_cols_idx_list = [self.input_col_idx['match_id'],
                                self.input_col_idx['player_num'],
                                self.input_col_idx['hero_id'],
                                self.input_col_idx['account_id'],
                                self.input_col_idx['lane'],
                                self.input_col_idx['rank_tier'],
                                self.input_col_idx['patch'],
                                self.input_col_idx['duration'],
                                self.input_col_idx['win']]

        enemies_cols_idx_list = [self.input_col_idx['enemy_1'],
                                 self.input_col_idx['enemy_2'],
                                 self.input_col_idx['enemy_3'],
                                 self.input_col_idx['enemy_4'],
                                 self.input_col_idx['enemy_5']]

        hero_id_col_idx = self.input_col_idx['hero_id']
        gold_col_idx = self.input_col_idx['gold_t_10']
        lane_col_idx = self.input_col_idx['lane']

        # convert negative features
        negative_features = {k: v for k,v in self.impact_features.items() if v < 0}
        for feature_name, k in negative_features.items():
            feature_idx = self.input_col_idx[feature_name]
            new_values_arr = np.apply_along_axis(self.correct_negative_feature, 0, match_array[:, feature_idx])
            match_array[:, feature_idx] = new_values_arr

        hero_id_output_col_idx = output_cols_idx_list.index(self.input_col_idx['hero_id'])

        if match_array.shape[0] == 10:
            # calc impact
            f_vals = []

            for feature_name, k in self.impact_features.items():
                feature_idx = self.input_col_idx[feature_name]
                feature_arr = match_array[:, feature_idx]
                f_vals.append(feature_arr)

            vals = np.array(f_vals)

            m = np.nan_to_num(np.mean(vals, axis = 1))
            std = np.nan_to_num(np.std(vals, axis = 1))
            l = np.nan_to_num(np.transpose((np.transpose(vals) - m) / std))

            s = np.sum(np.transpose(l) - np.min(l, axis = 1), axis = 1)
            summ = sum(s)
            impact = np.array([el * 100 / summ for el in s])

            # calc 'won_lane'
            team_1_heroes = match_array[0, enemies_cols_idx_list]

            team_1_lanes_gold = {}
            team_2_lanes_gold = {}

            team_won_lane = {}

            for row in match_array:
                lane = row[lane_col_idx]
                hero = row[hero_id_col_idx]
                gold = row[gold_col_idx]

                if lane == 4:
                    continue

                if hero in team_1_heroes:
                    lane_gold = team_1_lanes_gold.get(lane, 0)
                    lane_gold += gold
                    team_1_lanes_gold[lane] = lane_gold
                else:
                    lane_gold = team_2_lanes_gold.get(lane, 0)
                    lane_gold += gold
                    team_2_lanes_gold[lane] = lane_gold

            gold_team_1 = team_1_lanes_gold.get(1, 0)
            gold_team_2 = team_2_lanes_gold.get(3, 0)
            team_won_lane[1] = 1 if gold_team_1 > gold_team_2 \
                else (0 if gold_team_1 == gold_team_2 else 2)

            gold_team_1 = team_1_lanes_gold.get(3, 0)
            gold_team_2 = team_2_lanes_gold.get(1, 0)
            team_won_lane[3] = 1 if gold_team_1 > gold_team_2 \
                else (0 if gold_team_1 == gold_team_2 else 2)

            gold_team_1 = team_1_lanes_gold.get(2, 0)
            gold_team_2 = team_2_lanes_gold.get(2, 0)
            team_won_lane[2] = 1 if gold_team_1 > gold_team_2 \
                else (0 if gold_team_1 == gold_team_2 else 2)

            team_won_lane[4] = 0

            won_list = []

            for row in match_array:
                lane = row[lane_col_idx]
                hero = row[hero_id_col_idx]
                hero_team = 1 if hero in team_1_heroes else 2

                team_won = team_won_lane.get(lane, 0)

                if team_won == 0:
                    won_list.append(0.5)
                else:
                    if hero_team == team_won:
                        won_list.append(1)
                    else:
                        won_list.append(0)

            won_lane = np.array(won_list)

            # set output vector
            match_array_output = match_array[:, output_cols_idx_list]
            match_array_output = np.append(match_array_output, impact.reshape((10, 1)), axis=1)
            match_array_output = np.append(match_array_output, won_lane.reshape((10, 1)), axis=1)

            heroes_tuples = [(row[hero_id_output_col_idx], row)
                             for row in match_array_output]

        return heroes_tuples


class DoFnDBUpdateHeroImpact(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnDBUpdateHeroImpact, self).__init__(**kwargs)

    def process(self, hero_tuple):
        import pandas as pd

        (hero_id, hero_data) = hero_tuple

        hero_df = pd.DataFrame(hero_data,
                               columns=['match_id',
                                        'player_num',
                                        'hero_id',
                                        'account_id',
                                        'lane',
                                        'rank_tier',
                                        'patch',
                                        'duration',
                                        'win',
                                        'impact',
                                        'won_lane'])

        self.update_by_delete_insert_with_df(table_name='ods.matches_heroes_impact',
                                             df=hero_df,
                                             partition_column_name='hero_id',
                                             partition_column_type=int,
                                             key_column_name='match_id',
                                             key_column_type=int)

        patches = set(hero_df['patch'].astype(int).tolist())

        return [(hero_id, p) for p in patches]


class DoFnKVHeroesPatches(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnKVHeroesPatches, self).__init__(**kwargs)
        self.heroes_list = kwargs.get('heroes', [])

    def process(self, patch):
        return [(h, patch) for h in self.heroes_list]


class DoFnHeroesPatchData(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnHeroesPatchData, self).__init__(**kwargs)

    def process(self, hero_patches):
        groups_data = []

        (hero_id, patches_list) = hero_patches
        unique_patches = set(patches_list)

        for patch in unique_patches:
            if patch:
                sql_patch = 'select hero_id, ' \
                            'patch, ' \
                            'lane, ' \
                            'rank_tier, ' \
                            'impact, ' \
                            'won_lane ' \
                            'from ods.matches_heroes_impact ' \
                            'where hero_id = {0} and patch = {1}'.format(hero_id, patch)

                if self.pg_db:
                    rows = self.pg_db.select_pure(sql_patch)
                    groups_data += [('{0}_{1}_{2}_{3}'.format(r[0], r[1], r[2], r[3]), r) for r in rows]

        return groups_data


class DoFnCalcTotalImpact(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnCalcTotalImpact, self).__init__(**kwargs)

    def process(self, group_tuple):
        import numpy as np

        result_tuples = []

        (group_key, values_tuples_list) = group_tuple
        matches_total_count = len(values_tuples_list)

        if matches_total_count > 0:
            hero_id = values_tuples_list[0][0]
            patch = values_tuples_list[0][1]
            lane = values_tuples_list[0][2]
            rank_tier = values_tuples_list[0][3]

            values_arr = np.array(values_tuples_list)
            values_sum = np.sum(values_arr, axis=0)

            impact_sum = values_sum[4]
            won_lane_count = values_sum[5]

            result_tuples.append((1, (hero_id, patch, lane, rank_tier, impact_sum, won_lane_count, matches_total_count)))

        return result_tuples



class DoFnDBUpdateHeroPatchImpact(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnDBUpdateHeroPatchImpact, self).__init__(**kwargs)

    def process(self, data_tuple):
        import pandas as pd

        (key, hero_patch_data) = data_tuple

        hero_patch_df = pd.DataFrame(hero_patch_data,
                                     columns=['hero_id',
                                              'patch',
                                              'lane',
                                              'rank_tier',
                                              'impact_total',
                                              'won_lane_count',
                                              'matches_total_count'])

        self.update_by_delete_insert_with_df(table_name='ods.matches_heroes_patches_impact',
                                             df=hero_patch_df,
                                             key_column_name='patch',
                                             key_column_type=int)

        return[1]


def list_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def get_heroes():
    import requests

    heroes = []

    result = requests.get(
        'https://raw.githubusercontent.com/odota/dotaconstants/master/build/heroes.json')
    result_json = result.json()

    for num, hero_data in result_json.items():
        hero_id = hero_data.get('id', 0)

        if hero_id > 0:
            heroes.append(hero_id)

    return heroes


def parse_string_date(string_date):
    return datetime.strptime(string_date, '%Y-%m-%d %H:%M:%S')


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--startDate',
                        dest='startDate',
                        type = parse_string_date,
                        default=(datetime.now() - timedelta(days=1)).replace(
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


    time_boundaries_list = collection_range_timestamps(startDate=known_args.startDate,
                                                       endDate=known_args.endDate,
                                                       delta=timedelta(hours=1),
                                                       return_as_list=True)

    # time_boundaries_list = collection_range_timestamps(startDate=datetime(2019, 1, 29, 0, 0, 0),
    #                                                   endDate=datetime(2019, 1, 29, 1, 0, 0),
    #                                                   delta=timedelta(hours=1),
    #                                                   return_as_list=True)


    pg = PostgresDb()

    # prepare pipelines and collections
    pipeline_options = PipelineOptions(pipeline_args=pipeline_args)

    heroes = get_heroes()

    # perform source data
    for time_boundaries_bulk_list in list_chunks(time_boundaries_list, 10):
        with beam.Pipeline(options=pipeline_options) as p:
            t_boundaries_sources = (p | 'next_time_boundaries_bulk' >> beam.Create(time_boundaries_bulk_list))

            (t_boundaries_sources
             | 'sql_prepare_matches_players' >> beam.ParDo(DoFnBuidQueryMatchesPlayers())
             | 'sql_execute_matches_players' >> beam.ParDo(DoFnExecuteSql(table_tag='matches_players', pg_db=pg))
             | 'matches_group_by_match_id' >> GroupByKey()
             | 'enrich_and_split_and_save' >> beam.ParDo(DoFnEnrichSplitByHeroes())
             | 'matches_group_by_hero_id' >> GroupByKey()
             | 'update_hero_impact' >> beam.ParDo(DoFnDBUpdateHeroImpact(pg_db=pg))
            )

    # aggregations
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'last_patches' >> collection_patches(pg_db=pg)
         | 'kv_patches_heroes' >> beam.ParDo(DoFnKVHeroesPatches(heroes=heroes))
         | 'patches_group_by_hero_id' >> GroupByKey()
         | 'sql_heroes_patches' >> beam.ParDo(DoFnHeroesPatchData(pg_db=pg))
         | 'heroes_patches_group_by_key' >> GroupByKey()
         | 'calc_total_impact' >> beam.ParDo(DoFnCalcTotalImpact(pg_db=pg))
         | 'aggregate_tuples' >> GroupByKey()
         | 'update_hero_patch_impact' >> beam.ParDo(DoFnDBUpdateHeroPatchImpact(pg_db=pg))
        )


if __name__ == '__main__':
    run(sys.argv[1:])
