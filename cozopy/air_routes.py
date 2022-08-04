import json
import time

import pandas as pd

from cozo import *


def remove_nan(d):
    return {k: v for (k, v) in d.items() if v is not None and v == v}


def insert_data(destroy_on_exit):
    db = CozoDb('db_test_flights', destroy_on_exit=destroy_on_exit)
    try:
        payload = [
            *DefAttrs('country')
            .code(Typing.string, index=Indexing.identity)
            .desc(Typing.string)(),

            *DefAttrs('continent')
            .code(Typing.string, index=Indexing.identity)
            .desc(Typing.string)(),

            *DefAttrs('airport')
            .iata(Typing.string, index=Indexing.identity)
            .icao(Typing.string, index=Indexing.indexed)
            .city(Typing.string, index=Indexing.indexed)
            .desc(Typing.string)
            .region(Typing.string, index=Indexing.indexed)
            .country(Typing.ref)
            .runways(Typing.int)
            .longest(Typing.int)
            .altitude(Typing.int)
            .lat(Typing.float)
            .lon(Typing.float)(),

            *DefAttrs('route')
            .src(Typing.ref)
            .dst(Typing.ref)
            .distance(Typing.int)(),

            *DefAttrs('geo')
            .contains(Typing.ref)(),
        ]
        start_time = time.time()
        tx_res = db.tx_attr(payload)['results']
        end_time = time.time()

        print(f'{len(tx_res)} attributes added in {(end_time - start_time) * 1000:.3f}ms')

        insertions = []
        nodes = pd.read_csv('air-routes-latest-nodes.csv', index_col=0)

        continents = nodes[nodes['~label'] == 'continent']
        for tuple in continents.itertuples():
            put_payload = remove_nan(
                {'_temp_id': str(tuple.Index), 'continent.code': tuple._3, 'continent.desc': tuple._5})
            insertions.append(Put(put_payload))

        country_idx = {}
        countries = nodes[nodes['~label'] == 'country']
        for tuple in countries.itertuples():
            put_payload = remove_nan({'_temp_id': str(tuple.Index), 'country.code': tuple._3, 'country.desc': tuple._5})
            country_idx[tuple._3] = str(tuple.Index)
            insertions.append(Put(put_payload))

        airports = nodes[nodes['~label'] == 'airport']
        for tuple in airports.itertuples():
            put_payload = remove_nan({
                '_temp_id': str(tuple.Index),
                'airport.iata': tuple._3,
                'airport.icao': None if tuple._4 == 'none' else tuple._4,
                'airport.desc': tuple._5,
                'airport.region': tuple._6,
                'airport.runways': int(tuple._7),
                'airport.longest': int(tuple._8),
                'airport.altitude': int(tuple._9),
                'airport.country': country_idx[tuple._10],
                'airport.city': tuple._11,
                'airport.lat': tuple._12,
                'airport.lon': tuple._13
            })
            insertions.append(Put(put_payload))

        edges = pd.read_csv('air-routes-latest-edges.csv', index_col=0)

        for tuple in edges[edges['~label'] == 'route'].itertuples():
            payload = remove_nan(
                {'route.src': str(tuple._1), 'route.dst': str(tuple._2), 'route.distance': int(tuple._4)})
            insertions.append(Put(payload))

        for tuple in edges[edges['~label'] == 'contains'].itertuples():
            payload = remove_nan({'_temp_id': str(tuple._1), 'geo.contains': str(tuple._2)})
            insertions.append(Put(payload))

        start_time = time.time()
        d_res = db.tx(insertions)['results']
        end_time = time.time()
        print(f'{len(d_res)} node data added in {(end_time - start_time) * 1000:.3f}ms')
        print(f'{len(d_res) / (end_time - start_time):.0f} insertions per second')
    except Exception as e:
        print(f'data already exists? {e}')
    return db


if __name__ == '__main__':
    db = insert_data(False)
    start_time = time.time()
    # res = db.run([Q(['?c', '?code', '?desc'],
    #                 Disj(T.country.code('?c', 'CU'),
    #                      Unify('?c', 10000239)),
    #                 T.country.code('?c', '?code'),
    #                 T.country.desc('?c', '?desc'))])
    res = db.run([Q([Max('?n')],
                    T.route.distance('?a', '?n'))])
    end_time = time.time()
    print(json.dumps(res, indent=2))
    print(f'{len(res)} results fetched in {(end_time - start_time) * 1000:.3f}ms')
