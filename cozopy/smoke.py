from cozo import *

if __name__ == '__main__':
    db = CozoDb('_test', destroy_on_exit=True)
    res = db.tx_attr(
        DefAttrs('person')
        .idd(Typing.string, index=Indexing.identity)
        .first_name(Typing.string, index=Indexing.indexed)
        .last_name(Typing.string, index=Indexing.indexed)
        .age(Typing.int, index=Indexing.indexed)
        .friend(Typing.ref, cardinality=Cardinality.many, index=Indexing.indexed)
        .weight(Typing.float, index=Indexing.indexed)
        .covid(Typing.bool)()
    )
    print(res)
    print(db.tx_attr([
        PutAttr('person.id', Typing.string, id=res['results'][0][0], cardinality=Cardinality.one,
                index=Indexing.identity),
        RetractAttr('person.covid', Typing.bool, id=res['results'][-1][0], cardinality=Cardinality.one, history=False,
                    index=Indexing.indexed),
    ]))
    print(db.tx([
        Put({'_temp_id': 'alice',
             'person.first_name': 'Alice',
             'person.age': 7,
             'person.last_name': 'Amorist',
             'person.id': 'alice_amorist',
             'person.weight': 25,
             'person.friend': 'eve'}),
        Put({'_temp_id': 'bob',
             'person.first_name': 'Bob',
             'person.age': 70,
             'person.last_name': 'Wonderland',
             'person.id': 'bob_wonderland',
             'person.weight': 100,
             'person.friend': 'alice'
             }),
        Put({'_temp_id': 'eve',
             'person.first_name': 'Eve',
             'person.age': 18,
             'person.last_name': 'Faking',
             'person.id': 'eve_faking',
             'person.weight': 50,
             'person.friend': [
                 'alice',
                 'bob',
                 {'person.first_name': 'Charlie',
                  'person.age': 22,
                  'person.last_name': 'Goodman',
                  'person.id': 'charlie_goodman',
                  'person.weight': 120,
                  'person.friend': 'eve'}
             ]
             }),
        Put({'_temp_id': 'david',
             'person.first_name': 'David',
             'person.age': 7,
             'person.last_name': 'Dull',
             'person.id': 'david_dull',
             'person.weight': 25,
             'person.friend': {
                 '_temp_id': 'george',
                 'person.first_name': 'George',
                 'person.age': 7,
                 'person.last_name': 'Geomancer',
                 'person.id': 'george_geomancer',
                 'person.weight': 25,
                 'person.friend': 'george'}
             }),
    ]))
    res = db.run([
        R.ff(['?a', '?b'],
             T.person.friend('?a', '?b')),
        R.ff(['?a', '?b'],
             T.person.friend('?a', '?c'),
             R.ff('?c', '?b')),
        Q(['?a'],
          T.person.first_name('?a', '?n'),
          T.person.first_name('?alice', 'Alice'),
          NotExists(R.ff('?a', '?alice'))
          ),
    ],
        out={'friend': Pull('?a', ['person.first_name'])})
    print(res)
