/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import {CozoDb} from "cozo-lib-wasm";

const db = CozoDb.new();
console.log(db);

function query(script, params) {
    const result = JSON.parse(db.run(script, params || ''));
    console.log(result);
    if (result.ok) {
        const headers = result.headers || [];
        const rows = result.rows.map(row => {
            let ret = {};
            for (let i = 0; i < row.length; ++i) {
                ret[headers[i] || `(${i})`] = row[i];
            }
            return ret
        });
        console.table(rows)
    } else {
        console.error(result.display || result.message)
    }

}

query(`
?[loving, loved] <- [['alice', 'eve'],
                     ['bob', 'alice'],
                     ['eve', 'alice'],
                     ['eve', 'bob'],
                     ['eve', 'charlie'],
                     ['charlie', 'eve'],
                     ['david', 'george'],
                     ['george', 'george']]

:replace love {loving, loved}
`);

query(`

alice_love_chain[person] := *love['alice', person]
alice_love_chain[person] := alice_love_chain[in_person], *love[in_person, person]

?[chained] := alice_love_chain[chained]
`)

query(`
?[person, page_rank] <~ PageRank(*love[])

:order -page_rank
`);

query(`
?[loved_by_e_not_b] := *love['eve', loved_by_e_not_b], not *love['bob', loved_by_e_not_b]
`);

query(`
?[] <- [[parse_timestamp(format_timestamp(now(), 'Asia/Shanghai')),]]
`);

query(`
?[] <- [[rand_uuid_v1()]]
`);