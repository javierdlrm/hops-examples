import logging
import time
from multiprocessing import Process, Queue
from typing import List, Dict
import kfserving
import hsfs
from .online_fs_config import OnlineFSConfig

logging.basicConfig(level=kfserving.constants.KFSERVING_LOGLEVEL)


def get_feature_vectors(fs, instances, storage):

    team_ids = [str(instance['team_id']) for instance in instances]
    logging.info("Getting feature vectors for teams" + ', '.join(team_ids))

    # One sql query

    logging.info("#1 -- Using one sql query (join, where)--")

    start = time.perf_counter()
    features_df = fs.sql("""SELECT t_fg.team_position, t_fg.team_budget, p_fg.average_player_rating, p_fg.average_player_worth, p_fg.average_player_age
                            FROM `teams_features_1` t_fg
                            LEFT JOIN `players_features_1` p_fg
                            ON t_fg.team_id = p_fg.team_id
                            WHERE t_fg.team_id IN (""" + ','.join(team_ids) + ")",
                         dataframe_type="pandas",
                         storage=storage)
    logging.info(f"-- {time.perf_counter() - start}")

    # Multiple sql queries sequentially

    logging.info("#2 -- Using multiple sql query sequentially--")

    start = time.perf_counter()

    teams_df = fs.sql("""SELECT t_fg.team_id, t_fg.team_position, t_fg.team_budget
                         FROM `teams_features_1` t_fg
                         WHERE t_fg.team_id IN (""" + ','.join(team_ids) + ")",
                      dataframe_type="pandas",
                      storage=storage)
    players_df = fs.sql("""SELECT p_fg.team_id, p_fg.average_player_rating, p_fg.average_player_worth, p_fg.average_player_age
                         FROM `players_features_1` p_fg
                         WHERE p_fg.team_id IN (""" + ','.join(team_ids) + ")",
                        dataframe_type="pandas",
                        storage=storage)
    features_df = teams_df.join(players_df.set_index('p_fg.team_id'), on='t_fg.team_id')
    features_df = features_df.drop(columns=['t_fg.team_id'])

    logging.info(f"-- {time.perf_counter() - start}")

    # Multiple sql queries parallelly

    logging.info("#3 -- Using multiple sql query parallelly--")

    start = time.perf_counter()

    def get_features(fs, query, queue):
        queue.put(fs.sql(query, dataframe_type="pandas", storage=storage))

    queue = Queue()
    procs = []
    dfs = []

    procs.append(Process(target=get_features, args=(fs, """SELECT t_fg.team_id, t_fg.team_position, t_fg.team_budget
                         FROM `teams_features_1` t_fg
                         WHERE t_fg.team_id IN (""" + ','.join(team_ids) + ")", queue)))
    procs.append(Process(target=get_features, args=(fs, """SELECT p_fg.team_id, p_fg.average_player_rating, p_fg.average_player_worth, p_fg.average_player_age
                         FROM `players_features_1` p_fg
                         WHERE p_fg.team_id IN (""" + ','.join(team_ids) + ")", queue)))

    for p in procs: p.start()

    for p in procs:
        df = queue.get()
        df.columns = [c.split('.')[1] for c in df.columns]
        dfs.append(df)

    for p in procs: p.join()

    features_df = dfs[0].join(dfs[1].set_index('team_id'), on='team_id')
    features_df = features_df.drop(columns=['team_id'])

    logging.info(f"-- {time.perf_counter() - start}")

    # for fv in features_df.to_numpy():
    #     logging.info(fv)

    # fake input for flower iris model
    return [[6.8, 2.8, 4.8, 1.4], [6.0, 3.4, 4.5, 1.6]]


def get_feature_store_connector(config):
    logging.info(f"Connecting to Online FS: host-{config.host}, port-{config.port}, project-{config.project}")
    conn = hsfs.connection(
        host=config.host,
        port=config.port,
        project=config.project,
        secrets_store=config.secrets_store,
        api_key_value=config.api_key)

    return conn.get_feature_store()


class OnlineFSTransformer(kfserving.KFModel):

    def __init__(self, name: str, predictor_host: str):
        super().__init__(name)
        logging.info(f"OnlineFSTransformer: Name {name} - Predictor {predictor_host}")
        self.predictor_host = predictor_host
        self.fs_config = OnlineFSConfig()
        self.fs = get_feature_store_connector(self.fs_config)

    def preprocess(self, inputs: Dict) -> Dict:
        feature_vectors = get_feature_vectors(self.fs, inputs['instances'], self.fs_config.storage)
        return {'instances': feature_vectors}

    def postprocess(self, inputs: List) -> List:
        return inputs
