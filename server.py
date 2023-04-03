from fastapi import FastAPI
from relations import downstream, local_confluence
from dictionary import dbdict
from rivers import MAX
import json
import sys

# uvicorn server:app --reload

app = FastAPI()

PREFIX = "/home/filip/Work/projects/world_rivers_map/"


@app.get("/downstream/{waterway_id}")
def get_downstream(waterway_id):
    node_to_waterways = dbdict(f"{PREFIX}node_to_waterways.sqlite", MAX)
    waterway_to_nodes = dbdict(f"{PREFIX}waterway_to_nodes.sqlite", MAX)
    waterway_to_river = dbdict(f"{PREFIX}waterway_to_river.sqlite", MAX)
    river_to_waterways = dbdict(f"{PREFIX}river_to_waterways.sqlite", MAX)
    river_to_local_confluence = dbdict(f"{PREFIX}river_to_local_confluence.sqlite", MAX)
    return json.dumps(downstream(int(waterway_id), node_to_waterways, waterway_to_nodes))


@app.get("/local_confluence/{waterway_id}")
def get_local_confluence(waterway_id):
    node_to_waterways = dbdict(f"{PREFIX}node_to_waterways.sqlite", MAX)
    waterway_to_nodes = dbdict(f"{PREFIX}waterway_to_nodes.sqlite", MAX)
    waterway_to_river = dbdict(f"{PREFIX}waterway_to_river.sqlite", MAX)
    river_to_waterways = dbdict(f"{PREFIX}river_to_waterways.sqlite", MAX)
    river_to_local_confluence = dbdict(f"{PREFIX}river_to_local_confluence.sqlite", MAX)
    return json.dumps(local_confluence(int(waterway_id), node_to_waterways, waterway_to_nodes, waterway_to_river, river_to_local_confluence))
