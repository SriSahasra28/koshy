import ast

class settings:
    def __init__(self):
        pass
    @staticmethod
    def get_db():
        config_file_path = 'background/security.txt'
        with open(config_file_path, 'r') as file:
            content = file.read()
        config = ast.literal_eval(content)
        username = config["username"]
        password = config["password"]
        hostname = config["hostname"]
        port = int(config["port"])
        database_name = config["database_name"]
        return [username, password, hostname, port, database_name]
    @staticmethod
    def get_api_details():
        config_file_path = 'background/security.txt'
        with open(config_file_path, 'r') as file:
            content = file.read()
        config = ast.literal_eval(content)
        api_key = config["api_key"]
        api_secret = config["api_secret"]
        userID = config["userID"]
        pwd = config["pwd"]
        totp_key = config["totp_key"]
        return [api_key, api_secret, userID, pwd, totp_key]
