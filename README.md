# Rivers World Map
Backend for an interactive map of world rivers and their confluences and downstreams.

## Starting the server

To start the server run the following command:

```
python server.py <path_to_config_file>
```

## Config file

This is a config file example:
```
{
    "secret": "base32totpsecret",
    "mbtiles_path": "./world.mbtiles",
    "port": 8000,
    "style_path": "./style.json",
    "dict_db_folder": "./data"
}
```