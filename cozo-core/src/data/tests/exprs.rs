/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{DataValue, DbInstance};

#[test]
fn expression_eval() {
    let db = DbInstance::default();

    let res = db
        .run_default(
            r#"
    ?[a] := a = if(2 + 3 > 1 * 99999, 190291021 + 14341234212 / 2121)
    "#,
        )
        .unwrap();
    assert_eq!(res.rows[0][0], DataValue::Null);

    let res = db
        .run_default(
            r#"
    ?[a] := a = if(2 + 3 > 1, true, false)
    "#,
        )
        .unwrap();
    assert_eq!(res.rows[0][0].get_bool().unwrap(), true);
}
