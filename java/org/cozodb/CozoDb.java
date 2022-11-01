package org.cozodb;

import java.util.*;

class CozoDb {
    private static native int openDb(String path);
    private static native boolean closeDb(int id);
    private static native String runQuery(int id, String script, String params);

    private int dbId;

    static {
        System.loadLibrary("cozo_java");
    }

    CozoDb(String path) {
        this.dbId = CozoDb.openDb(path);
    }

    String query(String script, String params) {
        return CozoDb.runQuery(this.dbId, script, params);
    }

    boolean close() {
        return CozoDb.closeDb(this.dbId);
    }

    public static void main(String[] args) {
        CozoDb db = new CozoDb("_test_db");
        System.out.println(db);
        System.out.println(db.query("?[] <- [[1, 2, 3]]", "{}"));
        try {
            System.out.println(db.query("?[z] <- [[1, 2, 3]]", "{}"));
        } catch (Exception e) {
            String msg = e.getMessage().substring("JNI call error!. Cause: ".length());
            System.out.println(msg);
        }
        System.out.println(db.close());
    }
}