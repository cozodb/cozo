declare module "cozo-node" {
  export class CozoDb {
    /**
     * Constructor
     *
     * @param engine:  defaults to 'mem', the in-memory non-persistent engine.
     *                 'sqlite', 'rocksdb' and maybe others are available,
     *                 depending on compile time flags.
     * @param path:    path to store the data on disk, defaults to 'data.db',
     *                 may not be applicable for some engines such as 'mem'
     * @param options: defaults to {}, ignored by all the engines in the published NodeJS artefact
     */
    constructor(engine?: string, path?: string, options?: object);

    /**
     * You must call this method for any database you no longer want to use:
     * otherwise the native resources associated with it may linger for as
     * long as your program runs. Simply `delete` the variable is not enough.
     */
    close(): void;

    /**
     * Runs a query
     *
     * @param script: the query
     * @param params: the parameters as key-value pairs, defaults to {}
     */
    run(script: string, params?: Record<string, any>): Promise<any>;

    /**
     * Export several relations
     *
     * @param relations:  names of relations to export, in an array.
     */
    exportRelations(relations: Array<string>): Promise<any>;

    /**
     * Import several relations.
     *
     * Note that triggers are _not_ run for the relations, if any exists.
     * If you need to activate triggers, use queries with parameters.
     *
     * @param data: in the same form as returned by `exportRelations`. The relations
     *              must already exist in the database.
     */
    importRelations(data: object): Promise<object>;

    /**
     * Backup database
     *
     * @param path: path to file to store the backup.
     */
    backup(path: string): Promise<any>;

    /**
     * Restore from a backup. Will fail if the current database already contains data.
     *
     * @param path: path to the backup file.
     */
    restore(path: string): Promise<object>;

    /**
     * Import several relations from a backup. The relations must already exist in the database.
     *
     * Note that triggers are _not_ run for the relations, if any exists.
     * If you need to activate triggers, use queries with parameters.
     *
     * @param path: path to the backup file.
     * @param rels: the relations to import.
     */
    importRelationsFromBackup(path: string, rels: Array<string>): Promise<any>;
  }
}
