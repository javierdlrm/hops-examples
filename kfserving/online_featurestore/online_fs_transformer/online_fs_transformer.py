import os
import logging
from typing import List, Dict
import kfserving
import hsfs

logging.basicConfig(level=kfserving.constants.KFSERVING_LOGLEVEL)

HOST = "HOST"
PORT = "PORT"
PROJECT = "PROJECT"
API_KEY = "API_KEY"
SECRETS_LOCAL = "local"

def get_feature_vectors(fs, instances):

    # Get feature groups
    # teams_fg = fs.get_feature_group('teams_features', 1)
    # players_fg = fs.get_feature_group('players_features', 1)
    
    # Select features
    # features = teams_fg.select(['team_position', 'team_budget']).join(
    #     players_fg.select(['average_player_worth', 'average_player_age', 'average_player_rating']))

    # features_df = fs.sql("SELECT t_fg.team_id, t_fg.team_position, t_fg.team_budget, p_fg.average_player_worth, p_fg.average_player_age, p_fg.average_player_rating FROM `teams_features_1` t_fg, `players_features_1` p_fg WHERE t_fg.team_id==1 AND p_fg.team_id==1")
    
    # df = fs.sql("SELECT t_fg.team_id, t_fg.team_position, t_fg.team_budget, p_fg.average_player_rating, p_fg.average_player_worth, p_fg.average_player_age FROM `teams_features_1` t_fg LEFT JOIN `players_features_1` p_fg ON t_fg.team_id = p_fg.team_id WHERE t_fg.team_id = 1")
    # instance = df.to_dict(orient="records")[0]

    logging.info("-------------------------------")

    logging.info("Getting team ids")
    team_ids = [instance['team_id'] for instance in instances]

    logging.info("Getting features")
    features_df = fs.sql("""SELECT t_fg.team_position, t_fg.team_budget, p_fg.average_player_rating, p_fg.average_player_worth, p_fg.average_player_age
                            FROM `teams_features_1` t_fg
                            LEFT JOIN `players_features_1` p_fg
                            ON t_fg.team_id = p_fg.team_id
                            WHERE t_fg.team_id IN (""" + team_ids.join(",") + ")")

    logging.info("Map to feature vectors")
    logging.info("Feature vectors:")
    for fv in features_df.to_numpy():
        logging.info(fv)

    return [[6.8,  2.8,  4.8,  1.4], [6.0,  3.4,  4.5,  1.6]] # fake input for flower iris model


def get_feature_store_config():
    return {
        "host": os.environ(HOST),
        "port": os.environ(PORT),
        "project": os.environ(PROJECT),
        "secrets_store": SECRETS_LOCAL,
        "api_key_value": os.environ(API_KEY)
    }


def get_feature_store_connector(config):
    logging.info("Connecting to Online FS...")
    conn = hsfs.connection(
        host=config.host,
        port=config.port,
        project=config.project,
        secrets_store=config.secrets_store,
        api_key_value=config.api_key_value)

    return conn.get_feature_store()


class OnlineFSTransformer(kfserving.KFModel):

    def __init__(self, name: str, predictor_host: str):
        super().__init__(name)
        self.predictor_host = predictor_host
        self.fs_config = get_feature_store_config()
        self.fs = get_feature_store_connector(fs_config)

    def preprocess(self, inputs: Dict) -> Dict:
        logging.info("Getting feature vectors for Teams [" + [i.team_id for i in inputs['instances']].join(', ') + "] ...")
        return {'instances': get_feature_vectors(self.fs, inputs['instances']) }
        
    def postprocess(self, inputs: List) -> List:
        return inputs