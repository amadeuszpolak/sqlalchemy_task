import csv
from sqlalchemy import (
    create_engine,
    MetaData,
    Integer,
    String,
    ForeignKey,
    Table,
    Column,
)


def get_rows(path):
    rows = []
    with open(path, "r") as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)
        for row in csv_reader:
            rows.append(row)
    return rows


def create_db():
    engine = create_engine("sqlite:///zad6_2.db", echo=True)
    if not (engine.has_table("stations") and engine.has_table("measures")):
        meta = MetaData()
        stations = Table(
            "stations",
            meta,
            Column("station", String, primary_key=True),
            Column("latitude", String),
            Column("longitude", String),
            Column("elevation", String),
            Column("name", String),
            Column("country", String),
            Column("state", String),
        )
        measures = Table(
            "measures",
            meta,
            Column("id", Integer, primary_key=True),
            Column("station", String, ForeignKey("stations.station")),
            Column("date", String),
            Column("precip", String),
            Column("tobs", String),
        )
        meta.create_all(engine)
        conn = engine.connect()
        rows_stations = get_rows("sqlalchemy_task/clean_stations.csv")
        rows_measures = get_rows("sqlalchemy_task/clean_measure.csv")
        for row in rows_stations:
            conn.execute(stations.insert().values(row))
        i = 0
        for row in rows_measures:
            i += 1
            row.insert(0, i)
            conn.execute(measures.insert().values(row))
    else:
        conn = engine.connect()

    return conn


if __name__ == "__main__":

    conn = create_db()
    result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
    for row in result:
        print(row)