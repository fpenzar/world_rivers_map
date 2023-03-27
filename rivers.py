import osmium
import sys

OSM_FILE = "/home/geolux/tiles/tilemaker/sources/croatia-latest.osm.pbf"

class WaterwaysHandler(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        # key=node_id, value=list(waterway_ids the node is present in)
        self.waterway_nodes = {}
        # key=waterway_id, value=(start_node_id, end_node_id)
        self.boundary_nodes = {}


    def way(self, w):
        if w.is_closed() or w.tags.get("waterway") not in ["river", "stream"]:
            return
        if w.tags.get("intermittent") == "yes":
            return
        for n in w.nodes:
            if n.ref not in self.waterway_nodes:
                self.waterway_nodes.update({n.ref: []})
            self.waterway_nodes[n.ref].append(w.id)
        self.boundary_nodes.update({w.id: (w.nodes[0].ref, w.nodes[-1].ref)})


if __name__ == "__main__":
    waterways = WaterwaysHandler()
    waterways.apply_file(OSM_FILE)

    # downstream
    # river_id = 350935692 # Bračana
    river_id = 604231222
    open_waterways = {river_id}
    closed_waterways = set()
    while len(open_waterways):
        river = open_waterways.pop()
        end_node = waterways.boundary_nodes[river][1]
        for adjacent_river in waterways.waterway_nodes[end_node]:
            if adjacent_river == river:
                continue
            if adjacent_river in closed_waterways or adjacent_river in open_waterways:
                continue
            # check rivers don't share the same end node
            adjacent_river_end_node = waterways.boundary_nodes[adjacent_river][1]
            if adjacent_river_end_node == end_node:
                continue
            open_waterways.add(adjacent_river)
        closed_waterways.add(river)
    print(closed_waterways)
