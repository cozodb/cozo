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
    const cb_id = db.registerCallback('test', (op, new_rows, old_rows) => {
        console.log(`${op} ${JSON.stringify(new_rows)} ${JSON.stringify(old_rows)}`)
    })

    await db.run(`?[a] <- [[1],[2],[3]] :create test {a}`);

    db.registerNamedRule('Pipipy', 1, async (inputs, options) => {
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

    console.log((await db.exportRelations(['test']))['test']['rows'])

    const tx = db.multiTransact(true);
    await tx.run(':create a {a}');
    await tx.run('?[a] <- [[1]] :put a {a}');
    try {
        await tx.run(':create a {a}')
    } catch (e) {
    }
    await tx.run('?[a] <- [[2]] :put a {a}')
    await tx.run('?[a] <- [[3]] :put a {a}')
    tx.commit()

    const res = await db.run('?[a] := *a[a]');
    console.log(res);

    db.unregisterCallback(cb_id)
    db.unregisterNamedRule('Pipipy')
})()

function sleep(ms) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve()
        }, ms);
    })
}