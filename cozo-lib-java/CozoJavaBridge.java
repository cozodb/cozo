package org.cozodb;

public class CozoJavaBridge {
    private static native int openDb(String kind, String path);
    private static native boolean closeDb(int id);
    private static native String runQuery(int id, String script, String params);
    private static native String exportRelations(int id, String rel);
    private static native String importRelations(int id, String data);
    private static native String backup(int id, String file);
    private static native String restore(int id, String file);

    static {
        System.loadLibrary("cozo_java");
    }

    public static void main(String[] args) {
        System.out.println("OK");
    }
}