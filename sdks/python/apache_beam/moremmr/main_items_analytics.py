import argparse
from datetime import date, datetime, timedelta

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import GroupByKey

from base_classes import DoFnBase
from file_storage import FileStorage
from postgres_db import PostgresDb


# requirements:
#beam/sdks/python/container/base_image_requirements.txt
#+
#beam/sdks/python/setup.py


def collection_range_timestamps(startDate, endDate, delta=timedelta(minutes=30), return_as_list=False):
    times = []

    current_date = startDate

    while current_date < endDate:
        date_from = str(current_date)
        date_to = str(current_date + delta)
        times.append(set([date_from, date_to]))
        current_date += delta

    return times if return_as_list else beam.Create(times)


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

    def process(self, partition_interval):
        queries = []

        start = None
        end = None

        for boundary in partition_interval:
            if start is None:
                start = boundary
            else:
                end = boundary

        if start is not None and end is not None:
            if end < start:
                start, end = end, start

            queries.append({'start_date': start,
                            'end_date': end,
                            'sql': 'select match_id, '
                                          'player_num, '
                                          'time_tick, '
                                          'key '
                                          'from ods.matches_players_purchase_log '
                                          'where match_start_time >= timestamp \'{0}\' ' 
                                          'and match_start_time < timestamp \'{1}\' '.format(start, end)})

        return queries


class DoFnQueryMatchesPlayers(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnQueryMatchesPlayers, self).__init__(**kwargs)

    def process(self, partition_interval):
        queries = []

        start = None
        end = None

        for boundary in partition_interval:
            if start is None:
                start = boundary
            else:
                end = boundary

        if start is not None and end is not None:
            if end < start:
                start, end = end, start

            queries.append({'start_date': start,
                             'end_date': end,
                             'sql': 'select match_id, '
                                    'player_num, '
                                    'hero_id, '
                                    'lane, '
                                    'duration, '
                                    'allies_array[1] as ally_1, '
                                    'allies_array[2] as ally_2, '
                                    'allies_array[3] as ally_3, '
                                    'allies_array[4] as ally_4, '
                                    'enemies_array[1] as enemy_1, '
                                    'enemies_array[2] as enemy_2, '
                                    'enemies_array[3] as enemy_3, '
                                    'enemies_array[4] as enemy_4, '
                                    'enemies_array[5] as enemy_5, '
                                    'gold_t[10] as gold_t_10, '
                                    'item_0, '
                                    'item_1, '
                                    'item_2, '
                                    'item_3, '
                                    'item_4, '
                                    'item_5, '
                                    'actions_per_min, '
                                    'kills, ' 
                                    'hero_damage_per_min_raw, '
                                    'tower_damage_raw, '
                                    'stuns, '
                                    'deaths, '
                                    'hero_healing_per_min_raw, '
                                    'observer_uses, '
                                    'assists, '
                                    'gold_per_min_raw, '
                                    'win '
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
        #start_date = query_data['start_date']
        #end_date = query_data['end_date']

        tuples = []
        if self.pg_db is not None:
            rows = self.pg_db.select_pure(sql)
            if rows:
                if table_tag == 'purchase_log':
                    tuples = [('{0}_{1}'.format(r[0], r[1]), r) for r in rows]
                else:
                    tuples = [(r[0], r) for r in rows]

        tuples.sort(key=lambda tup: tup[0])

        return tuples


class DoFnVectorizeStarters(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnVectorizeStarters, self).__init__(**kwargs)
        self.items_ref = kwargs.get('items_ref', {})
        self.items_codes = kwargs.get('items_codes', {})
        self.items_codes_size = 10

        self.input_col_idx = {'match_id': 0,
                              'player_num': 1,
                              'time_tick': 2,
                              'key': 3}

    def map_items_cost(self, items_key_array, items_ref):
        import numpy as np
        return np.array([items_ref.get(item_id, np.zeros(2))
                         for item_id in items_key_array
                         if items_ref.get(item_id, np.zeros(2))[0] > 0
                         and items_ref.get(item_id, np.zeros(2))[1] > 0])

    def vectorization_sum(self, items_key_array, items_codes):
        import numpy as np
        items_vector_codes_arr = np.array([items_codes.get(item_id, np.zeros(10))
                                           for item_id in items_key_array])

        return np.sum(items_vector_codes_arr, axis=0)

    def process(self, match_player_tuple):
        import numpy as np
        #import pandas as pd

        res_vector = np.zeros(2 + 6 + self.items_codes_size) # match_id + player_num + 6 items in bag + self.items_codes_size

        (key, match_player_data) = match_player_tuple

        key_features = str(key).split('_')
        match_id = key_features[0]
        player_num = int(key_features[1])

        player_bag_array = np.array(match_player_data)#.astype(float)

        starter_items_arr = player_bag_array[player_bag_array[:, self.input_col_idx['time_tick']].astype(int) < 0]

        # filter and vectorize starter bags
        key_col_idx = self.input_col_idx['key']
        item_id_cost_arr = np.apply_along_axis(self.map_items_cost, 0, starter_items_arr[:,key_col_idx], self.items_ref)

        if item_id_cost_arr.shape[0] > 1:
            total_bag_cost = sum(item_id_cost_arr[:, 1])

            if total_bag_cost <= 800:
                unique_items = np.unique(item_id_cost_arr[:, 0])

                if len(unique_items) > 6:
                    unique_items = unique_items[:6]

                vectorization_sum = np.apply_along_axis(self.vectorization_sum, 0, unique_items, self.items_codes)
                unique_items = np.pad(unique_items, (0, 6 - len(unique_items)), 'constant')

                res_vector = np.array([match_id, player_num])
                res_vector = np.append(res_vector, unique_items)
                res_vector = np.append(res_vector, vectorization_sum)

                ''''
                blob_name = 'matches_item_starters_player_num_{0}.csv'.format(player_num)
                df = pd.DataFrame(res_vector)
                self.append_df_to_blob(blob_name=blob_name, df=df.T)
                '''

        return [(0, res_vector)]


class DoFnCalcItemsPurchTimeMedian(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnCalcItemsPurchTimeMedian, self).__init__(**kwargs)
        self.items_ref = kwargs.get('items_ref', {})

        self.input_col_idx = {'match_id': 0,
                              'player_num': 1,
                              'time_tick': 2,
                              'key': 3}


    def process(self, player_num_tuple):
        import pandas as pd
        import numpy as np

        (key, player_num_data) = player_num_tuple
        player_bag_array = np.array(player_num_data)

        player_bag_array = player_bag_array[player_bag_array[:, self.input_col_idx['time_tick']].astype(int) >= 0]

        # calc average purch time for items
        items_purch_time = []
        for item_name, item_id_cost in self.items_ref.items():
            (item_id, item_cost) = item_id_cost
            item_times_arr = player_bag_array[(player_bag_array[:, self.input_col_idx['time_tick']].astype(int) >= 0) & (player_bag_array[:, self.input_col_idx['key']] == item_name)][:100]
            time_tick_col = item_times_arr[:, self.input_col_idx['time_tick']].astype(int)
            if time_tick_col.shape[0] > 0:
                median = np.percentile(time_tick_col, 50)
                items_purch_time.append(np.array([item_id, median]))
                player_bag_array = player_bag_array[(player_bag_array[:, self.input_col_idx['key']] != item_name)]

        # save
        blob_items_medianes = 'item_purch_time_median.csv'
        df = pd.DataFrame(items_purch_time)
        self.append_df_to_blob(blob_name=blob_items_medianes, df=df)

        return [1]


class DoFnSavePlayerNumStarters(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnSavePlayerNumStarters, self).__init__(**kwargs)

    def process(self, player_num_tuple):
        import uuid
        import pandas as pd

        (key, player_num_data) = player_num_tuple

        blob_name = 'players_num_starter_items_{0}.zip'.format(str(uuid.uuid4()))
        blob_name_log = 'players_num_starter_items_log.csv'

        self.push_array_to_blob(blob_name=blob_name,
                                array=[row for row in player_num_data if row[0] > 0],
                                compression=True)

        # save name of the fie to log
        df_log_file = pd.DataFrame([blob_name])
        self.append_df_to_blob(blob_name=blob_name_log, df=df_log_file)

        return [blob_name]


class DoFnEnrichSplitByHeroes(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnEnrichSplitByHeroes, self).__init__(**kwargs)

        self.items_codes = kwargs.get('items_codes', {})
        self.items_codes_size = 10

        self.input_col_idx = {'match_id': 0,
                                'player_num': 1,
                                'hero_id': 2,
                                'lane': 3,
                                'duration': 4,
                                'ally_1': 5,
                                'ally_2': 6,
                                'ally_3': 7,
                                'ally_4': 8,
                                'enemy_1': 9,
                                'enemy_2': 10,
                                'enemy_3': 11,
                                'enemy_4': 12,
                                'enemy_5': 13,
                                'gold_t_10': 14,
                                'item_0': 15,
                                'item_1': 16,
                                'item_2': 17,
                                'item_3': 18,
                                'item_4': 19,
                                'item_5': 20,
                                'actions_per_min': 21,
                                'kills': 22,
                                'hero_damage_per_min_raw': 23,
                                'tower_damage_raw': 24,
                                'stuns': 25,
                                'deaths': 26,
                                'hero_healing_per_min_raw': 27,
                                'observer_uses': 28,
                                'assists': 29,
                                'gold_per_min_raw': 30,
                                'win': 31}

        self.impact_features = {'kills': 1,
                                'assists': 1,
                                'actions_per_min': 1,
                                'hero_damage_per_min_raw': 1,
                                'tower_damage_raw': 1,
                                'stuns': 1,
                                'deaths': -1,
                                'hero_healing_per_min_raw': 1,
                                'observer_uses': 1
                                }

    def correct_negative_feature(self, axis_array):
        import numpy as np
        return np.power(axis_array + 1, -1)

    def get_code_vector(self, items_id_array, items_codes):
        import numpy as np
        return np.array([items_codes.get(item_id, np.zeros(10)) for item_id in items_id_array])

    def process(self, match_tuple):
        import numpy as np

        heroes_tuples = []

        (match_id, match_data) = match_tuple
        match_array = np.array(match_data).astype(float)

        output_cols_idx_list = [self.input_col_idx['match_id'],
                                self.input_col_idx['player_num'],
                                self.input_col_idx['hero_id'],
                                self.input_col_idx['lane'],
                                self.input_col_idx['duration'],
                                self.input_col_idx['gold_per_min_raw'],
                                self.input_col_idx['item_0'],
                                self.input_col_idx['item_1'],
                                self.input_col_idx['item_2'],
                                self.input_col_idx['item_3'],
                                self.input_col_idx['item_4'],
                                self.input_col_idx['item_5'],
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

        # vectorize item_ids
        item_sum_vector = np.zeros((match_array.shape[0], 10))
        for item_num in range(6):
            col_idx = self.input_col_idx['item_{0}'.format(item_num)]
            item_num_ids = match_array[:, col_idx].astype(int)
            item_code_vectors = np.apply_along_axis(self.get_code_vector, 0, item_num_ids, self.items_codes)

            item_sum_vector += item_code_vectors

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
            impact = [el * 100 / summ for el in s]
            '''
            score = np.zeros(10)

            for feature_name, k in self.impact_features.items():
                feature_idx = self.input_col_idx[feature_name]
                feature_arr = match_array[:, feature_idx]
                feature_mean = np.mean(feature_arr)
                feature_std = np.std(feature_arr)
                feature_min = min(feature_arr)

                bias_arr = np.nan_to_num(np.zeros(10) - feature_min if feature_std == 0 else (feature_arr - feature_mean)/feature_std - feature_min)

                score += bias_arr

            score_sum = sum(score)

            impact = np.nan_to_num(np.zeros(10) if score_sum == 0 else 100 * score / score_sum)
            '''

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
            team_won_lane[1] = 1 if gold_team_1 > gold_team_2 else (0 if gold_team_1 == gold_team_2 else 2)

            gold_team_1 = team_1_lanes_gold.get(3, 0)
            gold_team_2 = team_2_lanes_gold.get(1, 0)
            team_won_lane[3] = 1 if gold_team_1 > gold_team_2 else (0 if gold_team_1 == gold_team_2 else 2)

            gold_team_1 = team_1_lanes_gold.get(2, 0)
            gold_team_2 = team_2_lanes_gold.get(2, 0)
            team_won_lane[2] = 1 if gold_team_1 > gold_team_2 else (0 if gold_team_1 == gold_team_2 else 2)

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
            match_array_output = np.append(match_array_output, item_sum_vector, axis=1)
            match_array_output = np.append(match_array_output, impact.reshape((10, 1)), axis=1)
            match_array_output = np.append(match_array_output, won_lane.reshape((10, 1)), axis=1)

            heroes_tuples = [(row[hero_id_output_col_idx], row) for row in match_array_output]

            '''
            for f in self.impact_features:
                vals.append(self.get_feature_values(players, f[0], True if f[1] == -1 else False))

            l = [[(e - np.mean(el)) / (np.std(el)) for e in el] for el in vals]

            s = [0] * 10

            for el in l:
                min_val = min(el)
                for i, e in enumerate(el):
                    s[i] += 0 if np.isnan((e - min_val)) else (e - min_val)

            impact = []

            for ss in s:
                impact.append(self.game_weight(ss, s))

            # replace items by vectors
            for player_num, player_row in players.items():
                item_sum_vector = np.zeros(10)
                for i in range(6):
                    item_id = int(player_row['item_{0}'.format(i)])
                    item_code_vector = items_w2v_codes.get(item_id, np.zeros(10))
                    item_sum_vector += item_code_vector

                new_player = np.array(player_row[['match_id',
                                                  'player_num',
                                                  'hero_id',
                                                  'lane',
                                                  'duration',
                                                  'gold_per_min_raw',
                                                  'item_0',
                                                  'item_1',
                                                  'item_2',
                                                  'item_3',
                                                  'item_4',
                                                  'item_5',
                                                  'win']])

                new_player = np.append(new_player, [impact[player_num]])
                new_player = np.append(new_player, item_sum_vector)
                new_players.append(new_player)
                
            '''

        return heroes_tuples



class DoFnAppendModelHeroInput(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnAppendModelHeroInput, self).__init__(**kwargs)

    def process(self, hero_tuple):
        import pandas as pd

        (hero_id, hero_data) = hero_tuple
        hero_df = pd.DataFrame(hero_data)
        blob_hero_name = 'hero_{0}_data.csv'.format(int(hero_id))
        self.append_df_to_blob(blob_name=blob_hero_name, df=hero_df)

        return [blob_hero_name]



class DoFnGetHeroData(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnGetHeroData, self).__init__(**kwargs)

    def process(self, hero_id):
        blob_hero_name = 'hero_{0}_data.csv'.format(int(hero_id))
        df_hero = self.get_blob_as_df(blob_name=blob_hero_name,
                                      names=['match_id',
                                             'player_num',
                                             'hero_id',
                                             'lane',
                                             'duration',
                                             'gold_per_min',
                                             'item_0',
                                             'item_1',
                                             'item_2',
                                             'item_3',
                                             'item_4',
                                             'item_5',
                                             'win',
                                             'item_coder_1',
                                             'item_coder_2',
                                             'item_coder_3',
                                             'item_coder_4',
                                             'item_coder_5',
                                             'item_coder_6',
                                             'item_coder_7',
                                             'item_coder_8',
                                             'item_coder_9',
                                             'item_coder_10',
                                             'impact',
                                             'won_lane'])

        hero_data = (int(hero_id), tuple(df_hero.values.astype(float)))

        return [hero_data]


class DoFnRunModelFinalItems(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnRunModelFinalItems, self).__init__(**kwargs)

        self.clusters_count = kwargs.get('clusters_count', 2)
        self.item_sets_in_cluster = kwargs.get('item_sets_in_cluster', 5)
        self.minimal_input_vectors_count = kwargs.get('minimal_input_vectors_count', 10)
        self.item_median = kwargs.get('item_median', {})
        if self.minimal_input_vectors_count < self.clusters_count:
            self.minimal_input_vectors_count = self.clusters_count

        self.duration_categories = {1: (0,       20 * 60),
                                    2: (20 * 60, 30 * 60),
                                    3: (30 * 60, 45 * 60),
                                    4: (45 * 60, 1440 * 60)}

    def process(self, hero_data_tuple):
        import json
        import pandas as pd
        from sklearn.cluster import KMeans
        from sklearn.metrics import pairwise_distances_argmin_min, pairwise_distances

        class ModelClusterization(object):
            def __init__(self, X, n_cluster, feature_uses):
                self.X = X
                self.n_cluster = n_cluster
                self.feature_uses = feature_uses

            def run(self):
                clusterization = KMeans(n_clusters=self.n_cluster).fit(self.X[self.feature_uses])
                self.X['cluster'] = clusterization.labels_

                closest_list = pairwise_distances(clusterization.cluster_centers_, self.X[self.feature_uses])
                dist = []
                for i, c in enumerate(self.X['cluster'].values):
                    dist.append(closest_list[c][i])
                self.X['distance'] = dist

                return self.X

        (hero_id, hero_data_arr) = hero_data_tuple

        df_hero = pd.DataFrame(list(hero_data_arr), columns=['match_id',
                                                             'player_num',
                                                             'hero_id',
                                                             'lane',
                                                             'duration',
                                                             'gold_per_min',
                                                             'item_0',
                                                             'item_1',
                                                             'item_2',
                                                             'item_3',
                                                             'item_4',
                                                             'item_5',
                                                             'win',
                                                             'item_coder_1',
                                                             'item_coder_2',
                                                             'item_coder_3',
                                                             'item_coder_4',
                                                             'item_coder_5',
                                                             'item_coder_6',
                                                             'item_coder_7',
                                                             'item_coder_8',
                                                             'item_coder_9',
                                                             'item_coder_10',
                                                             'impact',
                                                             'won_lane'])

        dur_cat = {}
        result_stat = {'items_purchase_time': self.item_median}

        if df_hero is not None:
            for category, intrervals in self.duration_categories.items():
                filter = df_hero['duration'].between(intrervals[0], intrervals[1] - 1)
                dur_cat[category] = df_hero[filter]

            for category, df_input in dur_cat.items():
                if df_input.shape[0] < self.minimal_input_vectors_count:
                    continue

                df_input = df_input.reset_index()

                result_stat[category] = {}
                result_stat[category]['duration_interval_sec'] = list(self.duration_categories[category])

                hero_model = ModelClusterization(X=df_input,
                                                 n_cluster=self.clusters_count,
                                                 feature_uses=['impact',
                                                                'gold_per_min',
                                                                'item_coder_1',
                                                                'item_coder_2',
                                                                'item_coder_3',
                                                                'item_coder_4',
                                                                'item_coder_5',
                                                                'item_coder_6',
                                                                'item_coder_7',
                                                                'item_coder_8',
                                                                'item_coder_9',
                                                                'item_coder_10'])

                try:
                    result_df = hero_model.run()
                    clusters = set(result_df['cluster'].tolist())
                except Exception as ex:
                    clusters = set()

                for cluster in clusters:
                    cluster_df = result_df[result_df['cluster'] == cluster]
                    cluster_matches = [int(m) for m in cluster_df['match_id'].tolist()]
                    matches_count = cluster_df.shape[0]

                    winners_df = df_input[(df_input['win'] == 1) &
                                          (df_input['match_id'].isin(cluster_matches))]
                    winners_count = winners_df.shape[0]

                    win_rate = 0 if matches_count == 0 else 100.0 * winners_count / matches_count

                    if win_rate < 50:
                        continue

                    result_stat[category][cluster] = {}
                    result_stat[category][cluster]['win_rate'] = win_rate
                    result_stat[category][cluster]['matches_in_cluster'] = matches_count

                    cluster_top_matches = cluster_df.sort_values(by=['distance'])

                    item_sets = {}

                    for idx, row in cluster_top_matches.iterrows():
                        item_set_idx = len(item_sets.keys())
                        item_sets[item_set_idx] = [int(row['item_{0}'.format(k)]) for k in range(6) if row['item_{0}'.format(k)] > 0]

                        if len(item_sets.keys()) >= self.item_sets_in_cluster:
                            break

                    result_stat[category][cluster]['item_sets'] = item_sets

        model_result_blob = 'model_final_items_hero_{0}_result.json'.format(hero_id)

        if self.file_storage is not None:
            model_result_file = '/tmp/{0}'.format(model_result_blob)

            with open(model_result_file, 'w') as json_file:
                json.dump(result_stat, json_file)

            self.file_storage.put_file(blob_name=model_result_blob,
                                       local_file_path=model_result_file,
                                       remove_local_file_after=True)

        return [1]


def collection_items_starters(file_storage):
    items_starters_files = []
    starter_items_log_blob_name = 'players_num_starter_items_log.csv'
    df_starter_items_log = file_storage.get_blob_as_df(starter_items_log_blob_name, names=['blob_name'])

    for idx, row in df_starter_items_log.iterrows():
        starter_items_blob_name = str(row['blob_name']).strip()
        items_starters_files.append(starter_items_blob_name)

    return beam.Create(items_starters_files)


class DoFnGetItemsStarters(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnGetItemsStarters, self).__init__(**kwargs)

    def process(self, blob_name):
        df_starter_items = self.file_storage.get_blob_as_df(blob_name=blob_name,
                                                            names=['match_id',
                                                                  'player_num',
                                                                  'item_0',
                                                                  'item_1',
                                                                  'item_2',
                                                                  'item_3',
                                                                  'item_4',
                                                                  'item_5',
                                                                  'item_coder_1',
                                                                  'item_coder_2',
                                                                  'item_coder_3',
                                                                  'item_coder_4',
                                                                  'item_coder_5',
                                                                  'item_coder_6',
                                                                  'item_coder_7',
                                                                  'item_coder_8',
                                                                  'item_coder_9',
                                                                  'item_coder_10'])

        items_starters_tuples = [('{0}_{1}'.format(int(row[0]), int(row[1])), tuple(row))
                                  for inx, row in df_starter_items.iterrows()]

        return items_starters_tuples



class DoFnMapPlayerNumToHero(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnMapPlayerNumToHero, self).__init__(**kwargs)

    def process(self, hero_data_tuple):
        from numpy import isnan
        (hero_id, hero_data) = hero_data_tuple
        res_tuples = [('{0}_{1}'.format(0 if isnan(row[0]) else int(row[0]), 0 if isnan(row[1]) else int(row[1])),
                       (hero_id, -1 if isnan(row[12]) else int(row[12]), -1 if isnan(row[24]) else int(row[24])))
                       for row in hero_data if row[0] > 0] # row[0]='match_id, row[1]='player_num', row[12]='win, row[24]='won_lane'

        return res_tuples


class DoFnModelStarterItems(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnModelStarterItems, self).__init__(**kwargs)

        self.clusters_count = kwargs.get('clusters_count', 2)
        self.item_sets_in_cluster = kwargs.get('item_sets_in_cluster', 5)
        self.minimal_input_vectors_count = kwargs.get('minimal_input_vectors_count', 10)
        if self.minimal_input_vectors_count < self.clusters_count:
            self.minimal_input_vectors_count = self.clusters_count

    def process(self, hero_starters_tuple):
        import json
        import pandas as pd
        from sklearn.cluster import KMeans
        from sklearn.metrics import pairwise_distances_argmin_min, pairwise_distances

        class ModelClusterization(object):
            def __init__(self, X, n_cluster, feature_uses):
                self.X = X
                self.n_cluster = n_cluster
                self.feature_uses = feature_uses

            def run(self):
                clusterization = KMeans(n_clusters=self.n_cluster).fit(self.X[self.feature_uses])
                self.X['cluster'] = clusterization.labels_

                closest_list = pairwise_distances(clusterization.cluster_centers_, self.X[self.feature_uses])
                dist = []
                for i, c in enumerate(self.X['cluster'].values):
                    dist.append(closest_list[c][i])
                self.X['distance'] = dist

                return self.X

        (hero_id, hero_starter_data) = hero_starters_tuple

        df_hero = pd.DataFrame(hero_starter_data, columns =['match_id',
                                                             'player_num',
                                                             'item_0',
                                                             'item_1',
                                                             'item_2',
                                                             'item_3',
                                                             'item_4',
                                                             'item_5',
                                                             'item_coder_1',
                                                             'item_coder_2',
                                                             'item_coder_3',
                                                             'item_coder_4',
                                                             'item_coder_5',
                                                             'item_coder_6',
                                                             'item_coder_7',
                                                             'item_coder_8',
                                                             'item_coder_9',
                                                             'item_coder_10',
                                                             'win',
                                                             'won_lane'])


        df_hero = df_hero[(df_hero['match_id'] > 0) & (df_hero['won_lane'] > 0)]

        result_stat = {}

        if df_hero is not None:
            if df_hero.shape[0] > 0:
                df_input = df_hero.reset_index()

                hero_model = ModelClusterization(X=df_input,
                                                 n_cluster=self.clusters_count,
                                                 feature_uses=['won_lane',
                                                               'item_coder_1',
                                                               'item_coder_2',
                                                               'item_coder_3',
                                                               'item_coder_4',
                                                               'item_coder_5',
                                                               'item_coder_6',
                                                               'item_coder_7',
                                                               'item_coder_8',
                                                               'item_coder_9',
                                                               'item_coder_10'])

                try:
                    result_df = hero_model.run()
                    clusters = set(result_df['cluster'].tolist())
                except Exception as ex:
                    clusters = set()

                for cluster in clusters:
                    cluster_df = result_df[result_df['cluster'] == cluster]
                    cluster_matches = [int(m) for m in cluster_df['match_id'].tolist()]
                    matches_count = cluster_df.shape[0]

                    winners_df = df_input[(df_input['win'] == 1) &
                                          (df_input['match_id'].isin(cluster_matches))]
                    winners_count = winners_df.shape[0]

                    win_rate = 0 if matches_count == 0 else 100.0 * winners_count / matches_count

                    if win_rate < 50:
                        continue

                    result_stat[cluster] = {}
                    result_stat[cluster]['win_rate'] = win_rate
                    result_stat[cluster]['matches_in_cluster'] = matches_count

                    cluster_top_matches = cluster_df.sort_values(by=['distance'])

                    item_sets = {}

                    for idx, row in cluster_top_matches.iterrows():
                        item_set_idx = len(item_sets.keys())
                        item_sets[item_set_idx] = [int(row['item_{0}'.format(k)]) for k in range(6) if row['item_{0}'.format(k)] > 0]

                        if len(item_sets.keys()) >= self.item_sets_in_cluster:
                            break

                    result_stat[cluster]['item_sets'] = item_sets

        model_result_blob = 'model_starter_items_hero_{0}_result.json'.format(hero_id)

        if self.file_storage is not None:
            model_result_file = '/tmp/{0}'.format(model_result_blob)

            with open(model_result_file, 'w') as json_file:
                json.dump(result_stat, json_file)

            self.file_storage.put_file(blob_name=model_result_blob,
                                       local_file_path=model_result_file,
                                       remove_local_file_after=True)

        return [1]


class DoFnCleanDataFiles(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnCleanDataFiles, self).__init__(**kwargs)

    def process(self, partition_interval):
        start = None
        end = None
        table_tag = self.get_argument('table_tag', '')

        for boundary in partition_interval:
            if start is None:
                start = boundary
            else:
                end = boundary

        if start is not None and end is not None:
            if end < start:
                start, end = end, start

            if self.file_storage is not None:
                blob_name = '{0}_data_{1}_{2}.zip'.format(table_tag, start, end)
                self.file_storage.delete_blob(blob_name=blob_name)

        return [1]


class DoFnCleanHeroDataFiles(DoFnBase):
    def __init__(self, **kwargs):
        super(DoFnCleanHeroDataFiles, self).__init__(**kwargs)

    def process(self, hero_id):
        blob_names = ['hero_{0}_data.csv'.format(int(hero_id))]

        if self.file_storage is not None:
            for blob_name in blob_names:
                self.file_storage.delete_blob(blob_name)

        return [1]


def clean_rest_files(file_storage):
    starter_items_log_blob_name = 'players_num_starter_items_log.csv'
    df_files1 = file_storage.get_blob_as_df(starter_items_log_blob_name, names=['blob_name'])

    blob_names = [row['blob_name'] for idx, row in df_files1.iterrows()]
    blob_names.append('item_purch_time_median.csv')

    for blob_name in blob_names:
        file_storage.delete_blob(blob_name)

    return True


def refresh_items(file_storage=None):
    import requests

    res = False

    if file_storage is not None:
        result = requests.get('http://api.steampowered.com/IEconDOTA2_570/GetGameItems/v1?key=B7C37A410BC2E6F52C9E5B654CABA039')
        result_json = result.json()

        result_dict = result_json.get('result', {})
        items_list = result_dict.get('items', [])

        rows = []

        for item_data in items_list:
            item_id = item_data.get('id', 0)
            item_name = item_data.get('name', '')
            item_cost = item_data.get('cost', 0)

            if item_name:
                item_name = item_name.replace('item_', '')

            rows.append((item_id, item_name, item_cost))

        blob_name = 'items.zip'
        res = file_storage.put_array_to_blob(blob_name=blob_name, array=rows, compression=True)

    return res


def starters_join_player_num_and_hero(merged_tuple):
    from numpy import zeros, concatenate
    (key, merged_data) = merged_tuple

    hero_id_list = merged_data.get('player_hero_map', [])
    player_data_list = merged_data.get('players_num', [])

    (hero_id, win, won_lane) = (0, -1, -1) if len(hero_id_list) < 1 else hero_id_list[0]
    player_data = zeros(18) if len(player_data_list) < 1 else player_data_list[0]

    res_tuple = (hero_id, tuple(concatenate((player_data, win, won_lane), axis=None)))

    return res_tuple


def list_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def run(argv=None):
    from numpy import isnan

    parser = argparse.ArgumentParser()

    parser.add_argument('--output', dest='output', required=True,
                        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    fs = FileStorage('items-analytics')
    pg = PostgresDb()

    # prepare pipelines and collections
    pipeline_options = PipelineOptions(pipeline_args)


    heroes_collection1 = collection_heroes()
    time_boundaries1 = collection_range_timestamps(datetime(2019, 1, 29, 0, 0, 0),
                                                   datetime(2019, 2, 2, 23, 59, 59))

    # clean data files
    with beam.Pipeline(options=pipeline_options) as p:
        t_boundaries_cleaning = (p | time_boundaries1)
        (t_boundaries_cleaning | 'clean_purch_log_files' >> beam.ParDo(DoFnCleanDataFiles(table_tag='purchase_log',
                                                                                          file_storage=fs)))
        (t_boundaries_cleaning | 'clean_matches_players_files' >> beam.ParDo(DoFnCleanDataFiles(table_tag='matches_players',
                                                                                                file_storage=fs)))

    # clean hero data files
    with beam.Pipeline(options=pipeline_options) as p:
        heroes_cleaning = (p | heroes_collection1)
        (heroes_cleaning | 'clean_hero_data_files' >> beam.ParDo(DoFnCleanHeroDataFiles(file_storage=fs)))

    # clean rest files
    clean_rest_files(file_storage=fs)


    time_boundaries_list = collection_range_timestamps(startDate=datetime(2019, 1, 29, 0, 0, 0),
                                                       endDate=datetime(2019, 2, 2, 23, 59, 59),
                                                       delta=timedelta(hours=1),
                                                       return_as_list=True)

    #time_boundaries_list = collection_range_timestamps(startDate=datetime(2019, 1, 29, 0, 0, 0),
    #                                                   endDate=datetime(2019, 1, 29, 1, 0, 0),
    #                                                   delta=timedelta(hours=1),
    #                                                   return_as_list=True)
    
    heroes_collection2 = collection_heroes()

    # items codes
    blob_name = 'items_w2v_codes.zip'
    df_items_codes = fs.get_blob_as_df(blob_name=blob_name, names=['item_id',
                                                                   'item_coder_1',
                                                                   'item_coder_2',
                                                                   'item_coder_3',
                                                                   'item_coder_4',
                                                                   'item_coder_5',
                                                                   'item_coder_6',
                                                                   'item_coder_7',
                                                                   'item_coder_8',
                                                                   'item_coder_9',
                                                                   'item_coder_10'
                                                                   ])
    items_w2v_codes = {int(row[0]): tuple(row[1:10 + 1]) for idx, row in df_items_codes.iterrows()}

    # items names id mapping
    refresh_items(file_storage=fs)
    blob_name = 'items.zip'
    df_items = fs.get_blob_as_df(blob_name=blob_name, names=['id', 'name', 'cost'])
    items_name_mapping = {row[1]: ([row[0], row[2]]) for idx, row in df_items.iterrows()}


    # perform source data
    for time_boundaries_bulk_list in list_chunks(time_boundaries_list, 10):
        with beam.Pipeline(options=pipeline_options) as p:
            t_boundaries_sources = (p | 'next_time_boundaries_bulk' >> beam.Create(time_boundaries_bulk_list))

            purch_log_data = (t_boundaries_sources
             | 'sql_prepare_purchase_log' >> beam.ParDo(DoFnQueryPurchLog())
             | 'sql_execute_purchase_log' >> beam.ParDo(DoFnExecuteSql(table_tag='purchase_log', pg_db=pg))
            )

            (purch_log_data
             | 'purch_log_median_change_tuple_key' >> beam.Map(lambda x: (x[1][1], x[1]))
             | 'purch_log_median_group_by_player_num' >> GroupByKey()
             | 'calc_save_items_purch_time_median' >> beam.ParDo(DoFnCalcItemsPurchTimeMedian(file_storage=fs,
                                                                                              items_ref=items_name_mapping))
            )

            (t_boundaries_sources
             | 'sql_prepare_matches_players' >> beam.ParDo(DoFnQueryMatchesPlayers())
             | 'sql_execute_matches_players' >> beam.ParDo(DoFnExecuteSql(table_tag='matches_players', pg_db=pg))
             | 'matches_group_by_match_id' >> GroupByKey()
             | 'enrich_and_split_and_save' >> beam.ParDo(DoFnEnrichSplitByHeroes(items_codes=items_w2v_codes))
             | 'matches_group_by_hero_id' >> GroupByKey()
             | 'append_model_hero_input' >> beam.ParDo(DoFnAppendModelHeroInput(file_storage=fs))
            )

            (purch_log_data
            | 'purch_log_group_by_match_id' >> GroupByKey()  # group all purchases of particular player_num in particular match_id
            | 'purch_log_vectorize_starters' >> beam.ParDo(DoFnVectorizeStarters(items_ref=items_name_mapping,
                                                                                 items_codes=items_w2v_codes,
                                                                                 file_storage=fs))
            | 'purch_log_group_all' >> GroupByKey()
            | 'purch_log_save_starters' >> beam.ParDo(DoFnSavePlayerNumStarters(file_storage=fs))
            )

            # (p | vectorized_starters | DoFnSavePlayerNumStarters(file_storage=fs, vectors=vectorized_starters_list))
            # | 'purch_log_starters_change_tuple_key' >> beam.Map(lambda x: (x[1], x))
            # | 'purch_log_starters_group_by_player_num' >> GroupByKey()
            # | 'purch_log_save_player_num_starters' >> beam.ParDo(DoFnSavePlayerNumStarters(file_storage=fs))
            # )


    # PURCH TIME MEDIANES
    item_median = {}
    blob_items_medianes = 'item_purch_time_median.csv'
    df_items_medianes = fs.get_blob_as_df(blob_name=blob_items_medianes, names=['item_id', 'median'])
    if df_items_medianes.shape[0] > 0:
        medianes_df = df_items_medianes.groupby(['item_id']).mean()
        !! тут надо проверить
        item_median = {int(row['item_id']): 0 if isnan(row['median']) else int(row['median']) for idx, row in
                       medianes_df.iterrows()}

    # MODELS
    with beam.Pipeline(options=pipeline_options) as p:
        heroes_data = (p
                       | 'heroes_collection_2' >> heroes_collection2
                       | 'get_hero_data' >> beam.ParDo(DoFnGetHeroData(file_storage=fs)))

        (heroes_data
         | 'run_model_final_items' >> beam.ParDo(DoFnRunModelFinalItems(clusters_count = 2,
                                                                        item_sets_in_cluster = 3,
                                                                        minimal_input_vectors_count = 10,
                                                                        item_median=item_median,
                                                                        file_storage=fs)))

        players_num = (p
                       | 'get_items_starters_blobs' >> collection_items_starters(file_storage=fs)
                       | 'get_items_starters_data' >> beam.ParDo(DoFnGetItemsStarters(file_storage=fs))
                       )

        player_hero_map = (heroes_data 
                           | 'map_hero_data_to_player_num' >> beam.ParDo(DoFnMapPlayerNumToHero()))

        ({'player_hero_map': player_hero_map, 'players_num': players_num}
         | 'starters_merging' >> beam.CoGroupByKey()
         | 'starters_join_player_num_and_hero' >> beam.Map(starters_join_player_num_and_hero)
         | 'heroes_starters_data_group_by_hero' >> GroupByKey()
         | 'run_model_starter_items' >> beam.ParDo(DoFnModelStarterItems(clusters_count=2,
                                                                         item_sets_in_cluster=3,
                                                                         minimal_input_vectors_count=10,
                                                                         file_storage=fs))
         )



if __name__ == '__main__':
    run()
