import osmium
import sys

OSM_FILE = "/home/geolux/tiles/tilemaker/sources/croatia-latest.osm.pbf"

CLASSES = ["river", "stream"]


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
            if len(self.waterway_nodes[n.ref]) < 2:
                continue
            self.body_nodes[w.id].append(n.ref)
        
        # append the last node
        self.body_nodes[w.id].append(w.nodes[-1].ref) 


if __name__ == "__main__":
    intersections = IntersectionsHandler()
    intersections.apply_file(OSM_FILE)

    waterways = WaterwaysHandler(intersections.waterway_nodes)
    waterways.apply_file(OSM_FILE)

    # downstream
    river_id = 350935692 # Bračana
    # river_id = 604231222
    open_waterways = {river_id}
    closed_waterways = set()
    while len(open_waterways):
        river = open_waterways.pop()
        for node in waterways.body_nodes[river]:
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
    print(closed_waterways)
