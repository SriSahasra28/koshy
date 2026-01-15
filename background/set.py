import os
import ast

class settings:
    def __init__(self):
        pass

    @staticmethod
    def _get_config_path():
        return os.path.join(os.path.dirname(__file__), 'security.txt')

    @staticmethod
    def get_db():
        config_file_path = settings._get_config_path()
        with open(config_file_path, 'r') as file:
            content = file.read()
        config = ast.literal_eval(content)
        return [
            config["username"],
            config["password"],
            config["hostname"],
            int(config["port"]),
            config["database_name"]
        ]

    @staticmethod
    def get_api_details():
        config_file_path = settings._get_config_path()
        with open(config_file_path, 'r') as file:
            content = file.read()
        config = ast.literal_eval(content)
        return [
            config["api_key"],
            config["api_secret"],
            config["userID"],
            config["pwd"],
            config["totp_key"]
        ]
