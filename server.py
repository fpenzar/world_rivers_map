from fastapi import FastAPI
from relations import downstream, local_confluence
from db_dict import dbdict
from rivers import MAX
import json

# uvicorn server:app --reload

app = FastAPI()


node_to_waterways = dbdict("node_to_waterways", MAX, check_same_thread=False)
waterway_to_nodes = dbdict("waterway_to_nodes", MAX, check_same_thread=False)
waterway_to_river = dbdict("waterway_to_river", MAX, check_same_thread=False)
river_to_waterways = dbdict("river_to_waterways", MAX, check_same_thread=False)
river_to_local_confluence = dbdict("river_to_local_confluence", MAX, check_same_thread=False)


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
