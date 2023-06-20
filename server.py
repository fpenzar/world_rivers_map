from fastapi import FastAPI, responses, Header, HTTPException
from relations import downstream, local_confluence
from db_dict import dbdict
from rivers import MAX
import json
from pymbtiles import MBtiles
from functools import lru_cache
from datetime import datetime, timedelta
from auth import TOTP_manager
from typing_extensions import Annotated
from typing import Union
from server_config import Config


# http://127.0.0.1:8000/docs

app = FastAPI()


config = Config()
totp_manager = TOTP_manager(config.secret)

node_to_waterways = dbdict(config.dict_db_folder, "node_to_waterways", MAX, check_same_thread=False)
waterway_to_nodes = dbdict(config.dict_db_folder, "waterway_to_nodes", MAX, check_same_thread=False)
waterway_to_river = dbdict(config.dict_db_folder, "waterway_to_river", MAX, check_same_thread=False)
river_to_waterways = dbdict(config.dict_db_folder, "river_to_waterways", MAX, check_same_thread=False)
river_to_local_confluence = dbdict(config.dict_db_folder, "river_to_local_confluence", MAX, check_same_thread=False)
waterway_to_confluence = dbdict(config.dict_db_folder, "waterway_to_confluence", MAX, check_same_thread=False)


@lru_cache(maxsize=100)
def cached_downstream(waterway_id):
    if not waterway_id.isdigit():
        return json.dumps([])
    return json.dumps(downstream(int(waterway_id), node_to_waterways, waterway_to_nodes))


@lru_cache(maxsize=100)
def cached_local_confluence(waterway_id):
    if not waterway_id.isdigit():
        return json.dumps([])
    return json.dumps(local_confluence(int(waterway_id), node_to_waterways,
                                        waterway_to_nodes, waterway_to_river, 
                                        river_to_waterways, river_to_local_confluence))

@lru_cache(maxsize=100)
def cached_confluence(waterway_id):
    if not waterway_id.isdigit():
        return json.dumps([])
    if not int(waterway_id) in waterway_to_confluence:
        return json.dumps([])
    return waterway_to_confluence[int(waterway_id)]


@lru_cache(maxsize=1000)
def cached_data(z, x, y):
    if not z.isdigit() or not x.isdigit() or not y.isdigit():
        return None
    
    if int(z) == 0:
        z = '1'
    elif int(z) > 14:
        z == '14'

    inverted_y = str(2**int(z) - 1 - int(y))
    with MBtiles(config.mbtiles_path) as src:
        tile_data = src.read_tile(z, x, inverted_y)
    if tile_data is None:
        return None

    expires_datetime = datetime.utcnow() + timedelta(days=7)
    expires_str = expires_datetime.strftime("%a, %d %b %Y %H:%M:%S GMT")
    headers = {"Content-Encoding": "gzip", "Access-Control-Allow-Origin": "*", "Expires": expires_str}

    return responses.Response(content=tile_data, media_type="application/x-protobuf", headers=headers)


@lru_cache(maxsize=1)
def cached_style():
    with open(config.style_path, "r") as file:
        json_file = json.loads(file.read())
    return json_file


@app.get("/downstream/{waterway_id}")
def get_downstream(waterway_id, totp_token: Annotated[Union[str, None], Header()] = None):
    if not totp_manager.verify(totp_token):
        raise HTTPException(status_code=401, detail="Invalid or missing TOTP token")
    return cached_downstream(waterway_id)


@app.get("/local_confluence/{waterway_id}")
def get_local_confluence(waterway_id, totp_token: Annotated[Union[str, None], Header()] = None):
    if not totp_manager.verify(totp_token):
        raise HTTPException(status_code=401, detail="Invalid or missing TOTP token")
    return cached_local_confluence(waterway_id)


@app.get("/confluence/{waterway_id}")
def get_confluence(waterway_id, totp_token: Annotated[Union[str, None], Header()] = None):
    if not totp_manager.verify(totp_token):
        raise HTTPException(status_code=401, detail="Invalid or missing TOTP token")
    return cached_confluence(waterway_id)


@app.get("/data/{z}/{x}/{y}.pbf")
def get_data(z, x, y, totp_token: Annotated[Union[str, None], Header()] = None):
    if not totp_manager.verify(totp_token):
        raise HTTPException(status_code=401, detail="Invalid or missing TOTP token")
    return cached_data(z, x, y)    


@app.get("/styles/style.json")
def get_json_style(totp_token: Annotated[Union[str, None], Header()] = None):
    if not totp_manager.verify(totp_token):
        raise HTTPException(status_code=401, detail="Invalid or missing TOTP token")
    return cached_style()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", reload=True, workers=8, port=config.port)
