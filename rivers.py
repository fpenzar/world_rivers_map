import osmium
import sys
import time
from dictionary import dbdict


FILE = "croatia-latest.osm"
PREFIX = "/home/geolux/tiles/world_rivers_map"
OSM_FILE = f"{PREFIX}/sources/{FILE}.pbf"
OSM_FILE_TRANSFORMED = f"{PREFIX}/transformed/{FILE}"
CSV_FILE = "/home/geolux/tiles/world_rivers_map/confluences.csv"

CLASSES = ["river", "stream", "canal"]


# only return nodes that have more than one waterway associted with them
# TODO this should be cleared
def clear_waterway_nodes(waterway_nodes):
    return {key: value for key, value in waterway_nodes.items() if len(value) >= 2}

MAX = 1000

def filter(w):
    if w.is_closed() or w.tags.get("waterway") not in CLASSES:
        return False
    if w.tags.get("intermittent") == "yes":
        return False
    return True


class IntersectionsHandler(osmium.SimpleHandler):

    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        # key=node_id, value=list(waterway_ids the node is present in)
        self.waterway_nodes = {}
        # self.waterway_nodes = dbdict("waterway_nodes", MAX)

    def way(self, w):
        if not filter(w):
            return
        for n in w.nodes:
            if n.ref not in self.waterway_nodes:
                self.waterway_nodes[n.ref] = []
            self.waterway_nodes[n.ref].append(w.id)


class WaterwaysHandler(osmium.SimpleHandler):

    def __init__(self, waterway_nodes):
        osmium.SimpleHandler.__init__(self)
        # key=node_id, value=list(waterway_ids the node is present in)
        self.waterway_nodes = waterway_nodes
        # key=waterway_id, value=[start_node_id, intersection_node_1, intersection_node_2, ..., end_node_id]
        # self.body_nodes = {}
        self.body_nodes = dbdict("body_nodes", MAX)


    def way(self, w):
        if not filter(w):
            return
        # add the start node
        # self.body_nodes.update({w.id: [w.nodes[0].ref]})
        self.body_nodes[w.id] = [w.nodes[0].ref]
        for index, n in enumerate(w.nodes):
            # skip first and last node as they are always added
            if index == 0 or index == len(w.nodes) - 1:
                continue
            # only add nodes that have more than one waterway
            if n.ref not in self.waterway_nodes:
                continue
            self.body_nodes[w.id].append(n.ref)
        
        # append the last node
        self.body_nodes[w.id].append(w.nodes[-1].ref)


class RiverHandler(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        # key=waterway_id, value=river_id
        # self.waterway_to_river = {}
        self.waterway_to_river = dbdict("waterway_to_river", MAX)
    

    def relation(self, r):
        if r.tags.get("waterway") != "river":
            return
        for member in r.members:
            self.waterway_to_river[member.ref] = r.id


class ConfluenceHandler(osmium.SimpleHandler):
    # calculate confluences

    def __init__(self, waterway_nodes, body_nodes, csv):
        osmium.SimpleHandler.__init__(self)
        self.csv = csv
        # key=node_id, value=list(waterway_ids the node is present in)
        self.waterway_nodes = waterway_nodes
        # key=waterway_id, value=[start_node_id, intersection_node_1, intersection_node_2, ..., end_node_id]
        self.body_nodes = body_nodes
        # key=id (from 0 upwards), value=list(waterway ids that are in the confluence)
        # self.confluences = {}
        self.confluences = dbdict("confluences", MAX)
        # key=waterway_id, value=confluence_id
        # self.waterway_to_confluence = {}
        self.waterway_to_confluence = dbdict("waterway_to_confluence", MAX)
        # confluence counter
        self.confluence_id_counter = 0    


    def write_confluence(self, w_id, confluence_id):
        self.csv.write(f"{w_id},{confluence_id}\n")
    

    def way(self, w):
        if not filter(w):
            return
        if w.id in self.waterway_to_confluence:
            self.write_confluence(w.id, self.waterway_to_confluence[w.id])
            return
        
        self.confluence_id_counter += 1
        open = {w.id}
        closed = set()
        while len(open):
            river = open.pop()
            for node in self.body_nodes.fast_get(river):
            # for node in self.body_nodes[river]:
                # only add nodes that have more than one waterway
                if node not in self.waterway_nodes:
                    continue
                # for adjacent_river in self.waterway_nodes.fast_get(node):
                for adjacent_river in self.waterway_nodes[node]:
                    if adjacent_river == river:
                        continue
                    if adjacent_river in closed or adjacent_river in open:
                        continue
                    open.add(adjacent_river)
            closed.add(river)
            # self.waterway_to_confluence.update({river: self.confluence_id_counter})
            self.waterway_to_confluence[river] = self.confluence_id_counter

        # self.confluences.update({self.confluence_id_counter: list(closed)})
        self.confluences[self.confluence_id_counter] = list(closed)
        print(self.confluence_id_counter)
        print(list(closed))
        print("########################")
        self.write_confluence(w.id, self.confluence_id_counter)



def downstream(river_id, waterway_nodes, body_nodes):
    open_waterways = {river_id}
    closed_waterways = set()
    while len(open_waterways):
        river = open_waterways.pop()
        for node in body_nodes.fast_get(river):
            # only process nodes that have more than one waterway
            if node not in waterway_nodes:
                continue
            for adjacent_river in waterway_nodes.fast_get(node):
                if adjacent_river == river:
                    continue
                if adjacent_river in closed_waterways or adjacent_river in open_waterways:
                    continue
                # the node in question cannot be the end node of a river
                adjacent_river_end_node = body_nodes.fast_get(adjacent_river)[-1]
                if adjacent_river_end_node == node:
                    continue
                open_waterways.add(adjacent_river)
        closed_waterways.add(river)
    return closed_waterways


def local_confluence(river_id, waterway_nodes, body_nodes, waterway_to_river):
    open_waterways = {river_id}
    closed_waterways = set()
    while len(open_waterways):
        waterway = open_waterways.pop()
        for node in body_nodes.fast_get(waterway):
            # only process nodes that have more than one waterway
            if node not in waterway_nodes:
                continue
            for adjacent_river in waterway_nodes.fast_get(node):
                if adjacent_river == waterway:
                    continue
                if adjacent_river in closed_waterways or adjacent_river in open_waterways:
                    continue
                # add if the segment belongs to the same river_relation
                if waterway in waterway_to_river and adjacent_river in waterway_to_river:
                    if waterway_to_river.fast_get(waterway) == waterway_to_river.fast_get(adjacent_river):
                        open_waterways.add(adjacent_river)
                        continue

                # not end node case
                if node != body_nodes.fast_get(waterway)[-1]:
                    # add if the shared node is adjacent_rivers end node
                    if body_nodes.fast_get(adjacent_river)[-1] == node:
                        open_waterways.add(adjacent_river)
                        continue
                    continue
                
                # end node case
                # skip if it is not the start node of the adjacent waterway
                if body_nodes.fast_get(adjacent_river)[0] != node:
                    continue
                # if it is end node of multiple waterways, then skip
                multiple_end_node = False
                for ww in waterway_nodes.fast_get(node):
                    if ww == waterway:
                        continue
                    if ww in closed_waterways or ww in open_waterways:
                        continue
                    if body_nodes.fast_get(ww)[-1] == node:
                        multiple_end_node = True
                        break
                if multiple_end_node:
                    continue
                open_waterways.add(adjacent_river)

        closed_waterways.add(waterway)
    return closed_waterways   


if __name__ == "__main__":
    if len(sys.argv) == 2:
        osm_file = sys.argv[1]
    else:
        osm_file = OSM_FILE
    
    write = True
    
    if write:
        start = time.time()

        intersections = IntersectionsHandler()
        print("Processing nodes")
        intersections.apply_file(osm_file)
        # intersections.apply_file("./sources/slovenia-latest.osm.pbf")

        # TODO
        # for better memory consumption
        # intersections.waterway_nodes = clear_waterway_nodes(intersections.waterway_nodes)

        waterways = WaterwaysHandler(intersections.waterway_nodes)
        print("Processing ways")
        waterways.apply_file(osm_file)
        # waterways.apply_file("./sources/slovenia-latest.osm.pbf")

        rivers = RiverHandler()
        print("Processing relations")
        rivers.apply_file(osm_file)
        # rivers.apply_file("./sources/slovenia-latest.osm.pbf")

        waterway_nodes = intersections.waterway_nodes
        body_nodes = waterways.body_nodes
        waterway_to_river = rivers.waterway_to_river

        # waterway_nodes = dbdict("waterway_nodes", MAX)
        # body_nodes = dbdict("body_nodes", MAX)
        # waterway_to_river = dbdict("waterway_to_river", MAX)

        with open(CSV_FILE, "a") as csv:
            confluence = ConfluenceHandler(waterway_nodes, body_nodes, csv)
            print("Calculating confluence")
            confluence.apply_file(osm_file)
            # confluence.apply_file("./sources/slovenia-latest.osm.pbf")

        end = time.time()
        print(f"Took {end - start} s for parsing data from {osm_file}")

    else:
        waterway_nodes = dbdict("waterway_nodes", MAX)
        body_nodes = dbdict("body_nodes", MAX)
        waterway_to_river = dbdict("waterway_to_river", MAX)


    river_id = 350935692 # Bračana
    river_id = 863577658 # u sloveniji
    river_id = 438527470 # mirna
    river_id = 22702834 # sava
    # river_id = 507633106 # amazon river
    start = time.time()
    # print(downstream(river_id, waterway_nodes, body_nodes))
    # end = time.time()
    # print(f"Took {end - start} s to calculate downstream for {river_id}")

    # start = time.time()
    # l_conf = local_confluence(river_id, waterway_nodes, body_nodes, waterway_to_river)
    # print(len(l_conf))
    end = time.time()
    print(f"Took {end - start} s to calculate local confluence for {river_id}")