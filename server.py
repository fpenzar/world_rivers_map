from fastapi import FastAPI, responses
from relations import downstream, local_confluence
from db_dict import dbdict
from rivers import MAX
import json
from pymbtiles import MBtiles
from fastapi.middleware.cors import CORSMiddleware
from functools import lru_cache

# allow access from machines in the local network
# uvicorn server:app --reload --host 0.0.0.0
# http://127.0.0.1:8000/docs

app = FastAPI()


node_to_waterways = dbdict("node_to_waterways", MAX, check_same_thread=False)
waterway_to_nodes = dbdict("waterway_to_nodes", MAX, check_same_thread=False)
waterway_to_river = dbdict("waterway_to_river", MAX, check_same_thread=False)
river_to_waterways = dbdict("river_to_waterways", MAX, check_same_thread=False)
river_to_local_confluence = dbdict("river_to_local_confluence", MAX, check_same_thread=False)
waterway_to_confluence = dbdict("waterway_to_confluence", MAX, check_same_thread=False)


@app.get("/downstream/{waterway_id}")
def get_downstream(waterway_id):
    if not waterway_id.isdigit():
        return json.dumps([])
    return json.dumps(downstream(int(waterway_id), node_to_waterways, waterway_to_nodes))


@app.get("/local_confluence/{waterway_id}")
def get_local_confluence(waterway_id):
    if not waterway_id.isdigit():
        return json.dumps([])
    return json.dumps(local_confluence(int(waterway_id), node_to_waterways,
                                        waterway_to_nodes, waterway_to_river, 
                                        river_to_waterways, river_to_local_confluence))


@app.get("/confluence/{waterway_id}")
def get_downstream(waterway_id):
    if not waterway_id.isdigit():
        return json.dumps([])
    return waterway_to_confluence[int(waterway_id)]


@app.get("/data/{z}/{x}/{y}.pbf")
@lru_cache(maxsize=1000)
def get_data(z, x, y):
    inverted_y = str(2**int(z) - 1 - int(y))
    with MBtiles('/home/geolux/tiles/tilemaker/world_v5.mbtiles') as src:
        tile_data = src.read_tile(z, x, inverted_y)

    headers = {"Content-Encoding": "gzip", "Access-Control-Allow-Origin": "*"}
    return responses.Response(content=tile_data, media_type="application/x-protobuf", headers=headers)


@app.get("/styles/style.json")
def get_json_style():
    with open("style.json", "r") as file:
        json_file = json.loads(file.read())
    return json_file
