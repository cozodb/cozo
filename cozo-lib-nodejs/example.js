/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

const {CozoDb} = require('.');

(async () => {
    const db = new CozoDb()
    try {
        const result = await db.run('?[a] <- [["hello"], ["world"]]');
        console.log(result.rows)
    } catch (e) {
        console.error(e)
    }
})()