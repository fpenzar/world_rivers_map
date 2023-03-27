import osmium
import sys
# TODO
# https://stackoverflow.com/questions/226693/python-disk-based-dictionary
# use shelve.open() instead of dicts

OSM_FILE = "/home/geolux/tiles/tilemaker/sources/croatia-latest.osm.pbf"

CLASSES = ["river", "stream", "canal"]


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


class ConfluenceHandler(osmium.SimpleHandler):

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
    

    def way(self, w):
        if not filter(w):
            return
        if w.id in self.waterway_to_confluence:
            return
        
        self.confluence_id_counter += 1
        open = {w.id}
        closed = set()
        while len(open):
            river = open.pop()
            for node in self.body_nodes[river]:
                for adjacent_river in self.waterway_nodes[node]:
                    if adjacent_river == river:
                        continue
                    if adjacent_river in closed or adjacent_river in open:
                        continue
                    open.add(adjacent_river)
            closed.add(river)
            self.waterway_to_confluence.update({w.id: self.confluence_id_counter})

        self.confluences.update({self.confluence_id_counter: list(closed)})
            


if __name__ == "__main__":
    intersections = IntersectionsHandler()
    intersections.apply_file(OSM_FILE)

    waterways = WaterwaysHandler(intersections.waterway_nodes)
    waterways.apply_file(OSM_FILE)

    confluence = ConfluenceHandler(intersections.waterway_nodes, waterways.body_nodes)
    confluence.apply_file(OSM_FILE)

    river_id = 350935692
    print(confluence.confluences[confluence.waterway_to_confluence[river_id]])

    # # downstream
    # river_id = 350935692 # Bračana
    # # river_id = 604231222
    # open_waterways = {river_id}
    # closed_waterways = set()
    # while len(open_waterways):
    #     river = open_waterways.pop()
    #     for node in waterways.body_nodes[river]:
    #         for adjacent_river in waterways.waterway_nodes[node]:
    #             if adjacent_river == river:
    #                 continue
    #             if adjacent_river in closed_waterways or adjacent_river in open_waterways:
    #                 continue
    #             # the node in question cannot be the end node of a river
    #             adjacent_river_end_node = waterways.body_nodes[adjacent_river][-1]
    #             if adjacent_river_end_node == node:
    #                 continue
    #             open_waterways.add(adjacent_river)
    #     closed_waterways.add(river)
    # print(closed_waterways)
