"""
Unit tests for OpenCellID Ghana Cellular Tower Ingestion & Spatial Resolver.
"""

import gzip
import io
import os
from geomesh.opencellid import OpenCellIDManager, get_opencellid_manager


def test_opencellid_initialization_and_seed(tmp_path):
    db_file = str(tmp_path / "test_cells.db")
    mgr = OpenCellIDManager(db_path=db_file)
    stats = mgr.get_stats()

    assert stats["total_towers_indexed"] >= 24
    assert stats["mcc"] == 620
    assert stats["country"] == "Ghana"
    assert any("MTN Ghana" in c for c in stats["carriers"])


def test_opencellid_keta_and_tema_lookups(tmp_path):
    db_file = str(tmp_path / "test_cells.db")
    mgr = OpenCellIDManager(db_path=db_file)

    # Cape St. Paul Keta Lighthouse (Cell 103)
    keta = mgr.lookup(cell_id=103, lac=4021, mnc=1)
    assert keta is not None
    assert keta["lat"] == 5.8000
    assert keta["lon"] == 0.9000
    assert "Keta" in keta["carrier_name"]

    # Tema Port Deepwater (Cell 301)
    tema = mgr.lookup(cell_id=301)
    assert tema is not None
    assert tema["lat"] == 5.6200
    assert "Tema" in tema["carrier_name"]


def test_opencellid_ingest_gzipped_csv(tmp_path):
    db_file = str(tmp_path / "test_cells.db")
    mgr = OpenCellIDManager(db_path=db_file)

    # Construct mock OpenCellID gzip CSV:
    # radio,mcc,net,area,cell,unit,lon,lat,range,samples,changeable,created,updated,averageSignal
    csv_rows = [
        "radio,mcc,net,area,cell,unit,lon,lat,range,samples,changeable,created,updated,averageSignal\n",
        "LTE,620,1,4021,9999,0,0.9250,5.8550,14000,10,1,1600000000,1600000000,0\n",
        "GSM,620,2,3012,8888,0,0.6120,5.7750,9000,5,1,1600000000,1600000000,0\n",
    ]
    raw_csv = "".join(csv_rows).encode("utf-8")
    compressed = gzip.compress(raw_csv)

    # Ingest directly through SQLite connection
    with mgr._get_connection() as conn:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as gz:
            text_stream = io.TextIOWrapper(gz, encoding="utf-8")
            import csv
            reader = csv.reader(text_stream)
            next(reader)  # skip header
            for row in reader:
                conn.execute("""
                    INSERT INTO opencellid_towers (radio, mcc, mnc, lac, cell_id, lon, lat, range_meters, carrier_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (row[0], int(row[1]), int(row[2]), int(row[3]), int(row[4]), float(row[6]), float(row[7]), int(row[8]), "MTN Custom"))
            conn.commit()

    # Verify newly ingested tower 9999
    res = mgr.lookup(cell_id=9999)
    assert res is not None
    assert res["lat"] == 5.8550
    assert res["lon"] == 0.9250


def test_singleton_opencellid():
    m1 = get_opencellid_manager()
    m2 = get_opencellid_manager()
    assert m1 is m2
