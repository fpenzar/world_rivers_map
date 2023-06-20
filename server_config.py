import sys
import json


config_file_example = {
    "secret": "base32totpsecret",
    "mbtiles_path": "./world.mbtiles",
    "port": 8000,
    "style_path": "./style.json",
    "dict_db_folder": "./data"
}

class Config:

    def __init__(self):
        if len(sys.argv) != 2:
            print("Wrong number of arguments! Usage:")
            print("python3 server.py <path_to_config_file>")
            exit(0)
        self.config_file_path = sys.argv[1]
        self.parse_file()
    
    
    def parse_file(self):
        with open(self.config_file_path) as file:
            contents = json.loads(file.read())
        
        required_config_file_fields = ["secret", "mbtiles_path", "port", "style_path", "dict_db_folder"]
        for field in required_config_file_fields:
            if field not in contents:
                self.wrong_config_file_format()
        
        self.secret = contents["secret"]
        self.mbtiles_path = contents["mbtiles_path"]
        self.port = contents["port"]
        self.style_path = contents["style_path"]
        self.dict_db_folder = contents["dict_db_folder"]
    

    def wrong_config_file_format(self):
        print("Wrong config file format!")
        print("Config file example:")
        print(json.dumps(config_file_example))
        exit(0)