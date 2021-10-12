from setup import setup_environment
from sqlalchemy.ext.declarative import declarative_base

engine = setup_environment.get_database()
base = declarative_base()
conn = engine.connect()

# Normalize arc of different lines with same segments equal
stops = "SELECT id, name, pumabus FROM nodes WHERE pumabus = 1;"
for row_stops in conn.execute(stops):
    id = str(row_stops.id)
    name = str(row_stops.name)
    get_lines_in_stop_A = "SELECT id, name, tiempo, from_stop, to_stop FROM arcs WHERE arco_a = 1 AND arco_tipo = 0 " \
                          "AND from_stop = " + str(id) + " ORDER BY name;"
    line_in_stop_a = conn.execute(get_lines_in_stop_A)

    for line_a in line_in_stop_a:
        get_lines_in_stop_B = "SELECT id, name, tiempo, from_stop, to_stop FROM arcs WHERE arco_a = 1 AND " \
                              "arco_tipo = 0 AND from_stop = " + str(id) + " ORDER BY name;"
        line_in_stop_b = conn.execute(get_lines_in_stop_B)
        for line_b in line_in_stop_b:
            if line_a.to_stop == line_b.to_stop:
                tiempo = line_a.tiempo
                update_flow = "UPDATE arcs SET tiempo = " + str(tiempo) + \
                              " WHERE id = " + str(line_b.id) + ";"
                print(tiempo, line_b.id, id)
                conn.execute(update_flow)

print('done')
