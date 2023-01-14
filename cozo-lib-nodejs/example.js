/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

const {Buffer} = require('node:buffer')
const {CozoDb} = require('.');

(async () => {
    const db = new CozoDb()
    try {
        const result = await db.run('?[a] <- [["hello"], ["world"], [$b]]', {b: Buffer.alloc(8, 255)});
        console.log(result.rows)
    } catch (e) {
        console.error(e)
    }
    const cb_id = db.register_callback('test', (op, new_rows, old_rows) => {
        console.log(`${op} ${JSON.stringify(new_rows)} ${JSON.stringify(old_rows)}`)
    })

    await db.run(`?[a] <- [[1],[2],[3]] :create test {a}`);

    db.register_named_rule('Pipipy', 1, async (inputs, options) => {
        console.log(`rule inputs: ${JSON.stringify(inputs)} ${JSON.stringify(options)}`)
        await sleep(1000);
        return inputs[0].map((row) => [row[0] * options.mul])
    })

    try {
        let r = await db.run(`
        rel[] <- [[1],[2]]
        
        ?[a] <~ Pipipy(rel[], mul: 3)
        `);
        console.log(r);
    } catch (e) {
        console.error(e.display);
    }
    db.unregister_callback(cb_id)
    db.unregister_named_rule('Pipipy')
})()

function sleep(ms) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve()
        }, ms);
    })
}