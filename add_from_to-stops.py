from setup import setup_environment
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = setup_environment.get_database()
conn = engine.connect()
base = declarative_base()

Session = sessionmaker(engine)
session = Session()
base.metadata.create_all(engine)

#########
# PART 1
#########
stops = "SELECT from_id, name, flow FROM arcs where h_arco = 1 order by name;"
for row_stops in conn.execute(stops):
    ruta = str(row_stops.name)
    from_id = str(row_stops.from_id)
    flow = row_stops.flow
    print(ruta, from_id, flow)
    arcs_to_update = "SELECT id FROM arcs WHERE from_id = (" \
                     "SELECT to_id FROM arcs WHERE name = '" + ruta + "' and h_arco = 1 and from_id = " + from_id + \
                     ");"
    for row_arc in conn.execute(arcs_to_update):
        update_flow = "UPDATE arcs SET flow = " + str(flow) + " WHERE id = " + str(row_arc.id) + ";"
        conn.execute(update_flow)


f_p_hour = "UPDATE arcs SET flow_p_hour = (60/frec_matu) * flow WHERE h_arco = 1;"
conn.execute(f_p_hour)

stops = "SELECT from_id, name, flow_p_hour FROM arcs where h_arco = 1 order by name;"
for row_stops in conn.execute(stops):
    ruta = str(row_stops.name)
    from_id = str(row_stops.from_id)
    flow_p_hour = row_stops.flow_p_hour
    print(ruta, from_id, flow_p_hour)
    arcs_to_update = "SELECT id FROM arcs WHERE from_id = (" \
                     "SELECT to_id FROM arcs WHERE name = '" + ruta + "' and h_arco = 1 and from_id = " + from_id + \
                     ");"
    for row_arc in conn.execute(arcs_to_update):
        update_flow = "UPDATE arcs SET flow_p_hour = " + str(flow_p_hour) + " WHERE id = " + str(row_arc.id) + ";"
        conn.execute(update_flow)


###########################
# FROM STOP
###########################
stops = "SELECT from_id, name, flow FROM arcs where h_arco = 1 order by name;"
for row_stops in conn.execute(stops):
    ruta = str(row_stops.name)
    from_id = str(row_stops.from_id)
    flow = row_stops.flow
    print(ruta, from_id, flow)
    arcs_to_update = "SELECT id FROM arcs WHERE from_id = (" \
                     "SELECT to_id FROM arcs WHERE name = '" + ruta + "' and h_arco = 1 and from_id = " + from_id + \
                     ");"
    for row_arc in conn.execute(arcs_to_update):
        update_from_stop = "UPDATE arcs SET from_stop = " + str(from_id) + " WHERE id = " + str(row_arc.id) + ";"
        conn.execute(update_from_stop)

###########################
# TO STOP (1st round)
###########################
stops = "SELECT from_id, name, flow FROM arcs where h_arco = 1 order by name;"
for row_stops in conn.execute(stops):
    ruta = str(row_stops.name)
    from_id = str(row_stops.from_id)
    flow = row_stops.flow
    print(ruta, from_id, flow)
    arcs_to_update = "SELECT id FROM arcs WHERE arco_a = 1 AND to_id = (" \
                     "SELECT from_id FROM arcs WHERE name = '" + ruta + "' and h_arco = 1 and from_id = " + from_id + \
                     ");"
    for row_arc in conn.execute(arcs_to_update):
        update_from_stop = "UPDATE arcs SET to_stop = " + str(from_id) + " WHERE id = " + str(row_arc.id) + ";"
        conn.execute(update_from_stop)


###########################
# TO STOP (2nd round)
###########################
stops = "SELECT from_id, name, flow FROM arcs where h_arco = 1 order by name;"
for row_stops in conn.execute(stops):
    ruta = str(row_stops.name)
    from_id = str(row_stops.from_id)
    flow = row_stops.flow
    print(ruta, from_id, flow)
    arcs_to_update = "SELECT id FROM arcs WHERE arco_a = 1 AND to_id = (" \
                     "SELECT to_id FROM arcs WHERE name = '" + ruta + "' and h_arco = 1 and from_id = " + from_id + \
                     ");"
    for row_arc in conn.execute(arcs_to_update):
        update_from_stop = "UPDATE arcs SET to_stop = " + str(from_id) + " WHERE id = " + str(row_arc.id) + ";"
        conn.execute(update_from_stop)


###########################
# TO STOP (3rd round)
###########################
stops = "SELECT from_id, name, flow, to_stop FROM arcs where nomebajo = 1 order by name;"
for row_stops in conn.execute(stops):
    ruta = str(row_stops.name)
    from_id = str(row_stops.from_id)
    to_stop = str(row_stops.to_stop)
    flow = row_stops.flow
    print(ruta, from_id, flow)
    arcs_to_update = "SELECT id FROM arcs WHERE nomebajo is NULL AND from_id = (" \
                     "SELECT from_id FROM arcs WHERE name = '" + ruta + "' and nomebajo is NULL and from_id = " + \
                     from_id + ");"
    for row_arc in conn.execute(arcs_to_update):
        update_from_stop = "UPDATE arcs SET to_stop = " + str(to_stop) + " WHERE id = " + str(row_arc.id) + ";"
        conn.execute(update_from_stop)


###########################
# FROM STOP (4rd round)
###########################
stops = "SELECT to_id, name, flow, from_stop, flow_p_hour, ruta FROM arcs where arco_tipo = 0 AND nomebajo is NULL;"
for row_stops in conn.execute(stops):
    to_id = str(row_stops.to_id)
    name = str(row_stops.name)
    flow = row_stops.flow
    from_stop = str(row_stops.from_stop)
    flow_p_hour = row_stops.flow_p_hour
    ruta = str(row_stops.ruta)
    print(ruta, from_id, flow)
    arcs_to_update = "SELECT id FROM arcs WHERE from_id = " + to_id + ";"
    for row_arc in conn.execute(arcs_to_update):
        update_from_stop = "UPDATE arcs SET from_stop = " + str(from_stop) + " WHERE id = " + str(row_arc.id) + ";"
        conn.execute(update_from_stop)
        update_flow = "UPDATE arcs SET flow = " + str(flow) + " WHERE id = " + str(row_arc.id) + ";"
        conn.execute(update_flow)
        update_flow_p_hour = "UPDATE arcs SET flow_p_hour = " + str(flow_p_hour) + " WHERE id = " + str(row_arc.id) + ";"
        conn.execute(update_flow_p_hour)