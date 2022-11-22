/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

const binary = require('@mapbox/node-pre-gyp');
const path = require('path');
const binding_path = binary.find(path.resolve(path.join(__dirname, './package.json')));
const native = require(binding_path);

class CozoDb {
    constructor(path) {
        this.db_id = native.open_db(path)
    }

    close() {
        native.close_db(this.db_id)
    }

    run(script, params) {
        return new Promise((resolve, reject) => {
            const params_str = JSON.stringify(params || {})
            native.query_db(this.db_id, script, params_str, (result_str) => {
                const result = JSON.parse(result_str);
                if (result.ok) {
                    resolve(result)
                } else {
                    reject(result)
                }
            })
        })
    }

    exportRelations(rels) {
        return new Promise((resolve, reject) => {
            const rels_str = JSON.stringify(rels);
            native.export_relations(rels_str, (result_str) => {
                const result = JSON.parse(result_str);
                if (result.ok) {
                    resolve(result)
                } else {
                    reject(result)
                }
            })
        })
    }

    importRelation(relation, data) {
        return new Promise((resolve, reject) => {
            const rels_str = JSON.stringify({relation, data});
            native.import_relation(rels_str, (result_str) => {
                const result = JSON.parse(result_str);
                if (result.ok) {
                    resolve(result)
                } else {
                    reject(result)
                }
            })
        })
    }

    backup(path) {
        return new Promise((resolve, reject) => {
            native.backup_db(path, (result_str) => {
                const result = JSON.parse(result_str);
                if (result.ok) {
                    resolve(result)
                } else {
                    reject(result)
                }
            })
        })
    }

    restore(path) {
        return new Promise((resolve, reject) => {
            native.restore_db(path, (result_str) => {
                const result = JSON.parse(result_str);
                if (result.ok) {
                    resolve(result)
                } else {
                    reject(result)
                }
            })
        })
    }
}

module.exports = {CozoDb: CozoDb}
