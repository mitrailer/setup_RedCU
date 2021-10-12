from setup import setup_environment
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = setup_environment.get_database()
conn = engine.connect()
base = declarative_base()

Session = sessionmaker(engine)
session = Session()
base.metadata.create_all(engine)


# terminal_stop = 1054
# terminal_stop = 441
# terminal_stop = 296
# terminal_stop = 255
# terminal_stop = 663
terminal_stop = 306
# terminal = "SELECT * FROM arcos WHERE name = 'Ruta 1' and (h_arco = 1 OR nomebajo = 1)"

# rutas = ['Ruta 1', 'Ruta 3', 'Ruta 5']
# rutas = ['Ruta 2', 'Ruta 4']
# rutas = ['Ruta 6', 'Ruta 7', 'Ruta 8']
# rutas = ['Ruta 9', 'Ruta 11']
# rutas = ['Ruta 10']
rutas = ['Ruta 13']

for ruta in rutas:
    next = terminal_stop
    list =[terminal_stop]
    print('{},{},{}'.format(0, next, ruta))
    for i in range(1,1000):
        terminal = "SELECT * FROM arcos WHERE name = '" + ruta + "' and (h_arco = 1 OR nomebajo = 1)"
        for row_stops in conn.execute(terminal):
            if row_stops.from_id == next:
                if i % 2 == 0:
                    if row_stops.to_id != terminal_stop:
                        # print(int(i/2), row_stops.to_id, row_stops.name)
                        print('{},{},{}'.format(int(i/2), row_stops.to_id, row_stops.name))
                        list.append(row_stops.to_id)
                next = row_stops.to_id
                break
        if next == terminal_stop:
            list.pop()
            list.clear()
            break
    # print(list)



