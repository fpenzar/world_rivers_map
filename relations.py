def downstream(river_id, node_to_waterways, waterway_to_nodes):
    if river_id not in waterway_to_nodes:
        return []
    open_waterways = {river_id}
    closed_waterways = set()
    while len(open_waterways):
        river = open_waterways.pop()
        for node in waterway_to_nodes.fast_get(river):
            # only process nodes that have more than one waterway
            if node not in node_to_waterways:
                continue
            for adjacent_river in node_to_waterways.fast_get(node):
                if adjacent_river == river:
                    continue
                if adjacent_river in closed_waterways or adjacent_river in open_waterways:
                    continue
                # the node in question cannot be the end node of a river
                adjacent_river_end_node = waterway_to_nodes.fast_get(adjacent_river)[-1]
                if adjacent_river_end_node == node:
                    continue
                open_waterways.add(adjacent_river)
        closed_waterways.add(river)
    return list(closed_waterways)


def local_confluence_old(river_id, node_to_waterways, waterway_to_nodes, waterway_to_river):
    open_waterways = {river_id}
    closed_waterways = set()
    while len(open_waterways):
        waterway = open_waterways.pop()
        for node in waterway_to_nodes.fast_get(waterway):
            # only process nodes that have more than one waterway
            if node not in node_to_waterways:
                continue
            for adjacent_river in node_to_waterways.fast_get(node):
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
                if node != waterway_to_nodes.fast_get(waterway)[-1]:
                    # add if the shared node is adjacent_rivers end node
                    if waterway_to_nodes.fast_get(adjacent_river)[-1] == node:
                        open_waterways.add(adjacent_river)
                        continue
                    continue
                
                # end node case
                # skip if it is not the start node of the adjacent waterway
                if waterway_to_nodes.fast_get(adjacent_river)[0] != node:
                    continue
                # if it is end node of multiple waterways, then skip
                multiple_end_node = False
                for ww in node_to_waterways.fast_get(node):
                    if ww == waterway:
                        continue
                    if ww in closed_waterways or ww in open_waterways:
                        continue
                    if waterway_to_nodes.fast_get(ww)[-1] == node:
                        multiple_end_node = True
                        break
                if multiple_end_node:
                    continue
                open_waterways.add(adjacent_river)

        closed_waterways.add(waterway)
    return closed_waterways


def local_confluence(waterway_id, node_to_waterways, waterway_to_nodes, 
                     waterway_to_river, river_to_local_confluence):
    if waterway_id not in waterway_to_nodes:
        return []
    open_waterways = {waterway_id}
    closed_waterways = set()
    while len(open_waterways):
        waterway = open_waterways.pop()
        
        # if the waterway belongs to another river
        if waterway in waterway_to_river:
            river_id = waterway_to_river.fast_get(waterway)
            # add the whole local confluence
            if river_id in river_to_local_confluence:
                for tributary in river_to_local_confluence.fast_get(river_id):
                    closed_waterways.add(tributary)
                continue

        for node in waterway_to_nodes.fast_get(waterway):
            # only process nodes that have more than one waterway
            if node not in node_to_waterways:
                continue
            for adjacent_river in node_to_waterways.fast_get(node):
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
                if node != waterway_to_nodes.fast_get(waterway)[-1]:
                    # add if the shared node is adjacent_rivers end node
                    if waterway_to_nodes.fast_get(adjacent_river)[-1] == node:
                        open_waterways.add(adjacent_river)
                        continue
                    continue
                
                # end node case
                # skip if it is not the start node of the adjacent waterway
                if waterway_to_nodes.fast_get(adjacent_river)[0] != node:
                    continue
                # if it is end node of multiple waterways, then skip
                multiple_end_node = False
                for ww in node_to_waterways.fast_get(node):
                    if ww == waterway:
                        continue
                    if ww in closed_waterways or ww in open_waterways:
                        continue
                    if waterway_to_nodes.fast_get(ww)[-1] == node:
                        multiple_end_node = True
                        break
                if multiple_end_node:
                    continue
                open_waterways.add(adjacent_river)

        closed_waterways.add(waterway)
    return list(closed_waterways)