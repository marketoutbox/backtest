from datetime import date
from pathlib import Path
import sys

import pytest
from fastapi.testclient import TestClient
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main as worker

class Result:
    def __init__(self, rows): self.rows=rows
    def fetchone(self): return self.rows[0]
    def fetchall(self): return self.rows

class Database:
    def __init__(self): self.calls=[]
    def __call__(self): return self
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def execute(self,sql,params=None):
        self.calls.append((sql,params))
        if sql.startswith('SELECT count(*) FROM ('): return Result([(8000,)])
        if sql.startswith('WITH page_keys'):
            limit,offset=params[-2:]
            return Result([(f'NSE_EQ|{i:05}', '1m',date(2022,1,1),date(2026,9,30),10000,20,f'STOCK{i}',date(2022,1,3),date(2026,9,30),20,20) for i in range(offset,offset+limit)])
        if sql.startswith('SELECT DISTINCT'): return Result([(f'NSE_EQ|{i:05}',f'STOCK{i}') for i in range(params[-1])])
        raise AssertionError(f'Unexpected query: {sql}')

@pytest.fixture
def fixture(monkeypatch):
    db=Database();monkeypatch.setattr(worker,'db',db);monkeypatch.setenv('WORKER_SECRET','test')
    def forbidden(*args,**kwargs): raise AssertionError('Metadata must not fetch prices or Upstox tokens')
    monkeypatch.setattr(worker,'available_tokens',forbidden);monkeypatch.setattr(worker,'read_archive_frame',forbidden)
    return TestClient(worker.app),db,{'X-Worker-Secret':'test'}

def test_default_archive_payload_is_bounded_and_paginated(fixture):
    client,db,headers=fixture
    assert client.get('/symbols').status_code==401
    first=client.get('/symbols',headers=headers).json()
    assert first['total']==8000 and first['limit']==50 and len(first['symbols'])==50
    assert db.calls[-1][0].startswith('WITH page_keys')
    assert 'LIMIT %s OFFSET %s' in db.calls[-1][0]
    second=client.get('/symbols?offset=50',headers=headers).json()
    assert second['symbols'][0]['instrument']!=first['symbols'][0]['instrument']
    assert len(second['symbols'])==50 and second['offset']==50


def test_filters_are_parameterized_and_literal_search(fixture):
    client,db,headers=fixture
    assert client.get('/symbols?instrument=NSE_EQ%7CTEST&interval=1m&search=M%26M',headers=headers).status_code==200
    query,params=db.calls[-1]
    assert params==['NSE_EQ|TEST','1m','M&M','M&M',50,0]
    assert 'c.instrument=%s' in query and 'c.interval=%s' in query
    client.get('/instruments?search=NSE_EQ%7C&limit=10',headers=headers)
    assert db.calls[-1][1]==('NSE_EQ|','NSE_EQ|',10)


def test_metadata_bounds_and_stock_selector(fixture):
    client,db,headers=fixture
    for url in ['/symbols?limit=101','/symbols?offset=-1','/symbols?limit=0','/instruments?limit=101']:
        assert client.get(url,headers=headers).status_code==422
    assert client.get('/symbols?interval=bad',headers=headers).status_code==400
    response=client.get('/instruments',headers=headers).json()
    assert len(response['instruments'])==50
    assert set(response['instruments'][0])=={'instrument','symbol'}
    assert client.get('/instruments').status_code==401
