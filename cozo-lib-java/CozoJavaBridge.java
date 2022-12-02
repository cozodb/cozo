package org.cozodb;

public class CozoJavaBridge {
    private static native int openDb(String engine, String path, String options);
    private static native boolean closeDb(int id);
    private static native String runQuery(int id, String script, String params);
    private static native String exportRelations(int id, String rel);
    private static native String importRelations(int id, String data);
    private static native String backup(int id, String file);
    private static native String restore(int id, String file);
    private static native String importFromBackup(int id, String data);
}