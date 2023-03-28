import osmium
import sys
import copy
# TODO
# https://stackoverflow.com/questions/226693/python-disk-based-dictionary
# use shelve.open() instead of dicts

OSM_FILE = "./sources/croatia-latest.osm.pbf"

CLASSES = ["river", "stream", "canal"]


# only return nodes that have more than one waterway associted with them
def clear_waterway_nodes(waterway_nodes):
    return {key: value for key, value in waterway_nodes.items() if len(value) >= 2}


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

    def way(self, w):
        if not filter(w):
            return
        for n in w.nodes:
            if n.ref not in self.waterway_nodes:
                self.waterway_nodes.update({n.ref: []})
            self.waterway_nodes[n.ref].append(w.id)


class WaterwaysHandler(osmium.SimpleHandler):

    def __init__(self, waterway_nodes):
        osmium.SimpleHandler.__init__(self)
        # key=node_id, value=list(waterway_ids the node is present in)
        self.waterway_nodes = waterway_nodes
        # key=waterway_id, value=[start_node_id, intersection_node_1, intersection_node_2, ..., end_node_id]
        self.body_nodes = {}


    def way(self, w):
        if not filter(w):
            return
        # add the start node
        self.body_nodes.update({w.id: [w.nodes[0].ref]}) 
        for index, n in enumerate(w.nodes):
            # skip first and last node as they are always added
            if index == 0 or index == len(w.nodes) - 1:
                continue
            # only add nodes that have more than one waterway
            if n.ref not in self.waterway_nodes:
                continue
            # only add nodes that have more than one waterway
            # if len(self.waterway_nodes[n.ref]) < 2:
            #     continue
            self.body_nodes[w.id].append(n.ref)
        
        # append the last node
        self.body_nodes[w.id].append(w.nodes[-1].ref)


class RiverHandler(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        # key=waterway_id, value=river_id
        self.waterway_to_river = {}
    

    def relation(self, r):
        if r.tags.get("waterway") != "river":
            return
        for member in r.members:
            self.waterway_to_river[member.ref] = r.id


# this is eating up most of the memory
class ConfluenceHandler(osmium.SimpleHandler):
    # calculate confluences

    def __init__(self, waterway_nodes, body_nodes):
        osmium.SimpleHandler.__init__(self)
        # key=node_id, value=list(waterway_ids the node is present in)
        self.waterway_nodes = waterway_nodes
        # key=waterway_id, value=[start_node_id, intersection_node_1, intersection_node_2, ..., end_node_id]
        self.body_nodes = body_nodes
        # key=id (from 0 upwards), value=list(waterway ids that are in the confluence)
        self.confluences = {}
        # key=waterway_id, value=confluence_id
        self.waterway_to_confluence = {}
        # confluence counter
        self.confluence_id_counter = 0
        # key=river_id, value=list(waterway ids that are in the river)
        self.rivers = {}
        # key=waterway_id, value=river_id
        self.waterway_to_river = {}
    

    def way(self, w):
        if not filter(w):
            return
        if w.id in self.waterway_to_confluence:
            return
        
        print(f"    {len(self.waterway_to_confluence)} / {len(self.body_nodes)} ({round(len(self.waterway_to_confluence) / len(self.body_nodes) * 100, 2)} %)", end="\r")
        self.confluence_id_counter += 1
        open = {w.id}
        closed = set()
        while len(open):
            river = open.pop()
            for node in self.body_nodes[river]:
                # only add nodes that have more than one waterway
                if node not in self.waterway_nodes:
                    continue
                for adjacent_river in self.waterway_nodes[node]:
                    if adjacent_river == river:
                        continue
                    if adjacent_river in closed or adjacent_river in open:
                        continue
                    open.add(adjacent_river)
            closed.add(river)
            self.waterway_to_confluence.update({river: self.confluence_id_counter})

        self.confluences.update({self.confluence_id_counter: list(closed)})


def downstream(river_id, waterways: WaterwaysHandler):
    open_waterways = {river_id}
    closed_waterways = set()
    while len(open_waterways):
        river = open_waterways.pop()
        for node in waterways.body_nodes[river]:
            # only process nodes that have more than one waterway
            if node not in waterways.waterway_nodes:
                continue
            for adjacent_river in waterways.waterway_nodes[node]:
                if adjacent_river == river:
                    continue
                if adjacent_river in closed_waterways or adjacent_river in open_waterways:
                    continue
                # the node in question cannot be the end node of a river
                adjacent_river_end_node = waterways.body_nodes[adjacent_river][-1]
                if adjacent_river_end_node == node:
                    continue
                open_waterways.add(adjacent_river)
        closed_waterways.add(river)
    return closed_waterways


def local_confluence(river_id, waterways: WaterwaysHandler, rivers: RiverHandler):
    open_waterways = {river_id}
    closed_waterways = set()
    while len(open_waterways):
        waterway = open_waterways.pop()
        for node in waterways.body_nodes[waterway]:
            # only process nodes that have more than one waterway
            if node not in waterways.waterway_nodes:
                continue
            for adjacent_river in waterways.waterway_nodes[node]:
                if adjacent_river == waterway:
                    continue
                if adjacent_river in closed_waterways or adjacent_river in open_waterways:
                    continue
                # add if the segment belongs to the same river_relation
                if waterway in rivers.waterway_to_river and adjacent_river in rivers.waterway_to_river:
                    if rivers.waterway_to_river[waterway] == rivers.waterway_to_river[adjacent_river]:
                        open_waterways.add(adjacent_river)
                        continue

                # not end node case
                if node != waterways.body_nodes[waterway][-1]:
                    # add if the shared node is adjacent_rivers end node
                    if waterways.body_nodes[adjacent_river][-1] == node:
                        open_waterways.add(adjacent_river)
                        continue
                    continue
                
                # end node case
                # skip if it is not the start node of the adjacent waterway
                if waterways.body_nodes[adjacent_river][0] != node:
                    continue
                # if it is end node of multiple waterways, then skip
                multiple_end_node = False
                for ww in waterways.waterway_nodes[node]:
                    if ww == waterway:
                        continue
                    if ww in closed_waterways or ww in open_waterways:
                        continue
                    if waterways.body_nodes[ww][-1] == node:
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
    
    intersections = IntersectionsHandler()
    print("Processing nodes")
    intersections.apply_file(osm_file)
    intersections.apply_file("./sources/slovenia-latest.osm.pbf")

    # TODO this is a bit questionable how useful it is
    # for better memory consumption
    intersections.waterway_nodes = clear_waterway_nodes(intersections.waterway_nodes)

    waterways = WaterwaysHandler(intersections.waterway_nodes)
    print("Processing ways")
    waterways.apply_file(osm_file)
    waterways.apply_file("./sources/slovenia-latest.osm.pbf")

    rivers = RiverHandler()
    print("Processing relations")
    rivers.apply_file(osm_file)
    rivers.apply_file("./sources/slovenia-latest.osm.pbf")
    
    confluence = ConfluenceHandler(intersections.waterway_nodes, waterways.body_nodes)
    print("Calculating confluence")
    confluence.apply_file(osm_file)
    confluence.apply_file("./sources/slovenia-latest.osm.pbf")

    # river_id = 350935692
    # print(confluence.confluences[confluence.waterway_to_confluence[river_id]])
    # print(len(confluence.confluences))
    # print(f"waterway_to_confluence: {len(confluence.waterway_to_confluence)}")
    # print(f"confluences: {len(confluence.confluences)}")
    # print(f"body_nodes: {len(waterways.body_nodes)}")
    # print(f"waterway_nodes: {len(intersections.waterway_nodes)}")


    river_id = 350935692 # Bračana
    river_id = 863577658 # u sloveniji
    river_id = 438527470 # mirna
    river_id = 22702834 # sava
    # print(downstream(river_id, waterways))
    print(local_confluence(river_id, waterways, rivers))