import osmium
import sys
import time
from db_dict import dbdict
from relations import local_confluence, downstream, local_confluence_old
import os
from consts import *
import json


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
        self.node_to_waterways = dbdict(DICT_DB_FOLDER, "node_to_waterways", MAX)

        self.processed = 0

    def way(self, w):
        if not filter(w):
            return
        if VERBOSE:
            print(f" Processed: {self.processed}", end="\r")
        for n in w.nodes:
            if n.ref not in self.node_to_waterways:
                self.node_to_waterways[n.ref] = []
            self.node_to_waterways[n.ref].append(w.id)
        self.processed += 1
    

    def clear_redundant_nodes(self):
        self.node_to_waterways.clear_redundant_nodes()


class WaterwaysHandler(osmium.SimpleHandler):

    def __init__(self, node_to_waterways):
        osmium.SimpleHandler.__init__(self)
        # key=node_id, value=list(waterway_ids the node is present in)
        self.node_to_waterways = node_to_waterways
        # key=waterway_id, value=[start_node_id, intersection_node_1, intersection_node_2, ..., end_node_id]
        self.waterway_to_nodes = dbdict(DICT_DB_FOLDER, "waterway_to_nodes", MAX)
        self.processed = 0


    def way(self, w):
        if not filter(w):
            return
        if VERBOSE:
            print(f" Processed: {self.processed}", end="\r")
        # add the start node
        self.waterway_to_nodes[w.id] = [w.nodes[0].ref]
        for index, n in enumerate(w.nodes):
            # skip first and last node as they are always added
            if index == 0 or index == len(w.nodes) - 1:
                continue
            # only add nodes that have more than one waterway
            if n.ref not in self.node_to_waterways:
                continue
            self.waterway_to_nodes[w.id].append(n.ref)
        
        # append the last node
        self.waterway_to_nodes[w.id].append(w.nodes[-1].ref)
        self.processed += 1


class RiverHandler(osmium.SimpleHandler):
    def __init__(self, waterway_to_nodes):
        osmium.SimpleHandler.__init__(self)
        self.waterway_to_nodes = waterway_to_nodes
        # key=waterway_id, value=river_id
        self.waterway_to_river = dbdict(DICT_DB_FOLDER, "waterway_to_river", MAX)
        # key=river_id, value=[ww_id1, ww_id2, ..., ww_id3]
        self.river_to_waterways = dbdict(DICT_DB_FOLDER, "river_to_waterways", MAX)
        self.processed = 0
    

    def relation(self, r):
        if r.tags.get("waterway") not in CLASSES:
            return
        if VERBOSE:
            print(f" Processed: {self.processed}", end="\r")
        members = []
        for member in r.members:
            # only add ways
            if member.type != "w":
                continue
            if member.ref not in self.waterway_to_nodes:
                continue
            self.waterway_to_river[member.ref] = r.id
            members.append(member.ref)
        self.river_to_waterways[r.id] = members
        


class ConfluenceHandler(osmium.SimpleHandler):
    # calculate confluences

    def __init__(self, node_to_waterways, waterway_to_nodes, csv, waterway_to_river, river_to_waterways):
        osmium.SimpleHandler.__init__(self)
        self.csv = csv
        # key=node_id, value=list(waterway_ids the node is present in)
        self.node_to_waterways = node_to_waterways
        self.waterway_to_river = waterway_to_river
        self.river_to_waterways = river_to_waterways
        # key=waterway_id, value=[start_node_id, intersection_node_1, intersection_node_2, ..., end_node_id]
        self.waterway_to_nodes = waterway_to_nodes
        # key=id (from 0 upwards), value=list(waterway ids that are in the confluence)
        self.confluences = dbdict(DICT_DB_FOLDER, "confluences", MAX)
        # key=waterway_id, value=confluence_id
        self.waterway_to_confluence = dbdict(DICT_DB_FOLDER, "waterway_to_confluence", MAX)
        # confluence counter
        self.confluence_id_counter = 0


        self.processed = 0


    def write_confluence(self, w_id, confluence_id):
        self.csv.write(f"{w_id},{confluence_id}\n")
    

    def way(self, w):
        if not filter(w):
            return
        if VERBOSE:
            print(f" Processed: {self.processed}", end="\r")
        if w.id in self.waterway_to_confluence:
            # self.write_confluence(w.id, self.waterway_to_confluence[w.id])
            return
        
        self.confluence_id_counter += 1
        open = {w.id}
        closed = set()
        while len(open):
            waterway = open.pop()

            # this only happens when only parts of the world are generated
            # and the river exceeds the borders
            if waterway not in self.waterway_to_nodes:
                closed.add(waterway)
                continue
            # if waterway belongs to the river, add the whole river
            if waterway in self.waterway_to_river:
                river_id = self.waterway_to_river.fast_get(waterway)
                for ww in self.river_to_waterways.fast_get(river_id):
                    if ww == waterway:
                        continue
                    if ww in open or ww in closed:
                        continue
                    open.add(ww)
            
            for node in self.waterway_to_nodes.fast_get(waterway):
                # only add nodes that have more than one waterway
                if node not in self.node_to_waterways:
                    continue
                for adjacent_river in self.node_to_waterways.fast_get(node):
                    if adjacent_river == waterway:
                        continue
                    if adjacent_river in closed or adjacent_river in open:
                        continue
                    open.add(adjacent_river)
            closed.add(waterway)
            self.waterway_to_confluence[waterway] = self.confluence_id_counter
            self.write_confluence(waterway, self.confluence_id_counter)

        self.confluences[self.confluence_id_counter] = list(closed)
        # self.write_confluence(w.id, self.confluence_id_counter)
        self.processed += 1


class LocalConfluenceHandler(osmium.SimpleHandler):

    def __init__(self, node_to_waterways, waterway_to_nodes, waterway_to_river, river_to_waterways):
        osmium.SimpleHandler.__init__(self)
        # key=node_id, value=list(waterway_ids the node is present in)
        self.node_to_waterways = node_to_waterways
        # key=waterway_id, value=[start_node_id, intersection_node_1, intersection_node_2, ..., end_node_id]
        self.waterway_to_nodes = waterway_to_nodes
        # key=waterway_id, value=river_id
        self.waterway_to_river = waterway_to_river
        # key=river_id, value=[ww_id1, ww_id2, ..., ww_id3]
        self.river_to_waterways = river_to_waterways

        # key=river_id, value=[local_confluence_w1, local_confluence_w2, ..., local_confluence_wn]
        self.river_to_local_confluence = dbdict(DICT_DB_FOLDER, "river_to_local_confluence", MAX)

        # for stats
        self.processed = 0
        self.num_to_process = len(self.waterway_to_river.keys())


    def way(self, w):
        if not filter(w):
            return
        if w.id not in self.waterway_to_river:
            return
        if VERBOSE:
            print(f" Processed: {self.processed}", end="\r")
        river_id = self.waterway_to_river[w.id]
        if river_id in self.river_to_local_confluence:
            return
        confluence = local_confluence(w.id, self.node_to_waterways, self.waterway_to_nodes, 
                                      self.waterway_to_river, self.river_to_waterways, self.river_to_local_confluence)
        self.river_to_local_confluence[river_id] = confluence
        self.processed += 1


def test():
    if len(sys.argv) >= 2:
        osm_file = sys.argv[1]
        if len(sys.argv) == 3:
            VERBOSE = (sys.argv[2] == "true" or sys.argv[2] == "True" or sys.argv[2] == "1")
    else:
        osm_file = OSM_FILE
    
    write = True
    
    if write:
        start = time.time()

        intersections = IntersectionsHandler()
        print("Processing nodes")
        intersections.apply_file(osm_file)
        # intersections.apply_file("./sources/slovenia-latest.osm.pbf")
        intersections.clear_redundant_nodes()

        waterways = WaterwaysHandler(intersections.node_to_waterways)
        print("Processing ways")
        waterways.apply_file(osm_file)
        # waterways.apply_file("./sources/slovenia-latest.osm.pbf")

        rivers = RiverHandler()
        print("Processing relations")
        rivers.apply_file(osm_file)
        # rivers.apply_file("./sources/slovenia-latest.osm.pbf")

        node_to_waterways = intersections.node_to_waterways
        waterway_to_nodes = waterways.waterway_to_nodes
        waterway_to_river = rivers.waterway_to_river
        river_to_waterways = rivers.river_to_waterways

        # node_to_waterways = dbdict("node_to_waterways", MAX)
        # waterway_to_nodes = dbdict("waterway_to_nodes", MAX)
        # waterway_to_river = dbdict("waterway_to_river", MAX)
        # river_to_waterways = dbdict("river_to_waterways", MAX)

        local_confluence_handler = LocalConfluenceHandler(node_to_waterways, waterway_to_nodes, waterway_to_river, river_to_waterways)
        print("Calculating local confluences")
        local_confluence_handler.apply_file(osm_file)
        # local_confluence_handler.apply_file("./sources/slovenia-latest.osm.pbf")
        river_to_local_confluence = local_confluence_handler.river_to_local_confluence

        with open(CSV_FILE, "w") as csv:
            confluence = ConfluenceHandler(node_to_waterways, waterway_to_nodes, csv, waterway_to_river, river_to_waterways)
            print("Calculating global confluences")
            confluence.apply_file(osm_file)
            # confluence.apply_file("./sources/slovenia-latest.osm.pbf")
        
        end = time.time()
        print(f"Took {end - start} s for parsing data from {osm_file}")

    else:
        node_to_waterways = dbdict(DICT_DB_FOLDER, "node_to_waterways", MAX)
        waterway_to_nodes = dbdict(DICT_DB_FOLDER, "waterway_to_nodes", MAX)
        waterway_to_river = dbdict(DICT_DB_FOLDER, "waterway_to_river", MAX)
        river_to_waterways = dbdict(DICT_DB_FOLDER, "river_to_waterways", MAX)
        river_to_local_confluence = dbdict(DICT_DB_FOLDER, "river_to_local_confluence", MAX)


    river_id = 350935692 # Bračana
    river_id = 863577658 # u sloveniji
    river_id = 438527470 # mirna
    river_id = 22702834 # sava
    # river_id = 507633106 # amazon river
    start = time.time()
    print(downstream(river_id, node_to_waterways, waterway_to_nodes))
    end = time.time()
    print(f"Took {end - start} s to calculate downstream for {river_id}")

    start = time.time()
    l_conf = local_confluence(river_id, node_to_waterways, waterway_to_nodes, 
                              waterway_to_river, river_to_waterways,river_to_local_confluence)
    print(l_conf)
    print(len(l_conf))
    end = time.time()
    print(f"Took {end - start} s to calculate local confluence for {river_id}")

    start = time.time()
    l_conf_old = local_confluence_old(river_id, node_to_waterways, waterway_to_nodes, waterway_to_river)
    print(len(l_conf_old))
    end = time.time()
    print(f"Took {end - start} s to calculate local confluence (old) for {river_id}")


def write(skip_nodes=False):
    files = os.listdir(sys.argv[1])
    files = [os.path.join(sys.argv[1], file) for file in files]

    start = time.time()
    if not skip_nodes:
        # intersections
        intersections = IntersectionsHandler()
        print("Processing nodes")
        for i, osm_file in enumerate(files):
            print(f" {i + 1}/{len(files)}", end="\r")
            intersections.apply_file(osm_file)
        
        # reduce the size of the nodes file
        intersections.clear_redundant_nodes()
        
        # waterways
        waterways = WaterwaysHandler(intersections.node_to_waterways)
        print("Processing ways")
        for i, osm_file in enumerate(files):
            print(f" {i + 1}/{len(files)}", end="\r")
            waterways.apply_file(osm_file)

        node_to_waterways = intersections.node_to_waterways
        waterway_to_nodes = waterways.waterway_to_nodes
        # rivers
        rivers = RiverHandler(waterway_to_nodes)
        print("Processing relations")
        for i, osm_file in enumerate(files):
            print(f" {i + 1}/{len(files)}", end="\r")
            rivers.apply_file(osm_file)
        waterway_to_river = rivers.waterway_to_river
        river_to_waterways = rivers.river_to_waterways

    node_to_waterways = dbdict(DICT_DB_FOLDER, "node_to_waterways", MAX)
    waterway_to_nodes = dbdict(DICT_DB_FOLDER, "waterway_to_nodes", MAX)
    waterway_to_river = dbdict(DICT_DB_FOLDER, "waterway_to_river", MAX)
    river_to_waterways = dbdict(DICT_DB_FOLDER, "river_to_waterways", MAX)

    # global confluences
    with open(CSV_FILE, "w") as csv:
        confluence = ConfluenceHandler(node_to_waterways, waterway_to_nodes, csv, waterway_to_river, river_to_waterways)
        print("Calculating global confluences")
        for i, osm_file in enumerate(files):
            print(f" {i + 1}/{len(files)}", end="\r")
            confluence.apply_file(osm_file)


    # local confluences
    local_confluence_handler = LocalConfluenceHandler(node_to_waterways, waterway_to_nodes, waterway_to_river, river_to_waterways)
    print("Calculating local confluences")
    for i, osm_file in enumerate(files):
        print(f" {i + 1}/{len(files)}", end="\r")
        local_confluence_handler.apply_file(osm_file)
    river_to_local_confluence = local_confluence_handler.river_to_local_confluence

    
    end = time.time()
    print(f"Took {end - start} s for parsing data")


if __name__ == "__main__":
    write(True)
    # test()
