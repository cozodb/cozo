import initCozoDb, { CozoDb } from "cyb-cozo-lib-wasm";

import initializeScript from "./migrations/schema.cozo";
import { async } from "rxjs";

const DB_NAME = "cozo-idb-demo";
const DB_STORE_NAME = "cozodb";

interface Column {
  column: string;
  type: "String" | "Int" | "Bool" | "Float";
  is_key?: boolean;
  index: number;
  is_default: boolean;
}

interface TableSchema {
  keys: string[];
  values: string[];
  columns: Record<string, Column>;
}

type DBValue = string | number | boolean;

interface IDBResult {
  headers: string[];
  rows: Array<Array<DBValue>>;
  ok: true;
}

interface IDBResultError {
  code: string;
  display: string;
  message: string;
  severity: string;
  ok: false;
}

type DBSchema = Record<string, TableSchema>;

interface DBResultWithColIndex extends IDBResult {
  index: Record<string, number>;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function withColIndex({ headers }: IDBResult): DBResultWithColIndex {
  const index = headers.reduce((acc, column, index) => {
    acc[column] = index;
    return acc;
  }, {} as Record<string, number>);

  return { ...this, index };
}

const toListOfObjects = <T extends Record<string, any>>({
  rows,
  headers,
}: IDBResult): T[] => {
  return rows.map((row) => {
    const obj: Partial<T> = {};
    row.forEach((value, index) => {
      const key = headers[index];
      obj[key] = value;
    });
    return obj as T;
  });
};

const mapObjectToArray = (
  obj: Record<string, DBValue>,
  columns: Column[]
): string => {
  return `[${columns
    .map((col) =>
      col.type === "String" ? `"${obj[col.column]}"` : obj[col.column]
    )
    .join(", ")}]`;
};

function CozoDbCommandFactory(dbSchema: DBSchema) {
  let schema = dbSchema;

  const generatePutCommand = (tableName: string): string => {
    const { keys, values } = schema[tableName];
    const hasValues = values.length > 0;

    return !hasValues
      ? `:put ${tableName} {${keys}}`
      : `:put ${tableName} {${keys} => ${values}}`;
  };

  const generateAtomCommand = (tableName: string, items: any[]): string => {
    const tableSchema = dbSchema[tableName];
    const colKeys = Object.keys(tableSchema.columns);
    const colValues = Object.values(tableSchema.columns);
    return `?[${colKeys.join(", ")}] <- [${items
      .map((item) => mapObjectToArray(item, colValues))
      .join(", ")}]`;
  };

  const generatePut = (tableName: string, array: any[][]) => {
    const atomCommand = generateAtomCommand(tableName, array);
    const putCommand = generatePutCommand(tableName);
    return `${atomCommand}\r\n${putCommand}`;
  };

  const generateGet = (tableName: string, conditionArr: string[] = []) => {
    const conditionsStr =
      conditionArr.length > 0 ? `, ${conditionArr.join(", ")} ` : "";
    const tableSchema = dbSchema[tableName];
    const queryKeys = Object.keys(tableSchema.columns);
    return `?[${queryKeys.join(", ")}] := *${tableName}{${queryKeys.join(
      ", "
    )}} ${conditionsStr}`;
  };

  return { generatePutCommand, generateAtomCommand, generatePut, generateGet };
}

function DbService() {
  let db: CozoDb | undefined;

  let dbSchema: DBSchema = {};
  let commandFactory: ReturnType<typeof CozoDbCommandFactory> | undefined;

  async function init(
    callback?: (writesCount: number) => void
  ): Promise<CozoDb> {
    if (db) {
      return db;
    }

    await initCozoDb();

    db = await CozoDb.new_from_indexed_db(DB_NAME, DB_STORE_NAME, callback);
    dbSchema = await initDbSchema();
    commandFactory = CozoDbCommandFactory(dbSchema);

    console.log("CozoDb schema initialized: ", dbSchema);

    return db;
  }

  const getRelations = async (): Promise<string[]> => {
    const result = await runCommand("::relations");
    if (result.ok !== true) {
      throw new Error(result.message);
    }

    return result.rows.map((row) => row[0] as string);
  };

  const initDbSchema = async (): Promise<DBSchema> => {
    let relations = await getRelations();

    if (relations.length === 0) {
      console.log("CozoDb: apply DB schema", initializeScript);
      runCommand(initializeScript);
      relations = await getRelations();
    }

    const schemasMap = await Promise.all(
      relations.map(async (table) => {
        const columnResult = await runCommand(`::columns ${table}`);
        if (!columnResult.ok) {
          throw new Error((columnResult as IDBResultError).message);
        }

        const fields = toListOfObjects<Column>(columnResult);
        const keys = fields.filter((c) => c.is_key).map((c) => c.column);
        const values = fields.filter((c) => !c.is_key).map((c) => c.column);
        const tableSchema: TableSchema = {
          keys,
          values,
          columns: fields.reduce((obj, field) => {
            obj[field.column] = field;
            return obj;
          }, {} as Record<string, Column>),
        };
        return [table, tableSchema];
      })
    );

    return Object.fromEntries(schemasMap);
  };

  const runCommand = async (
    command: string,
    immutable = false
  ): Promise<IDBResult | IDBResultError> => {
    if (!db) {
      throw new Error("DB is not initialized");
    }
    const resultStr = await db.run(command, "", immutable);
    const result = JSON.parse(resultStr);
    console.log("----> runCommand ", command, result);

    return result;
  };

  const put = async (
    tableName: string,
    array: any[][]
  ): Promise<IDBResult | IDBResultError> =>
    runCommand(commandFactory.generatePut(tableName, array));

  const get = (
    tableName: string,
    conditionArr: string[] = []
  ): Promise<IDBResult | IDBResultError> =>
    runCommand(commandFactory.generateGet(tableName, conditionArr), true);

  return {
    init,
    put,
    get,
    runCommand,
    getCommandFactory: () => commandFactory,
  };
}

export type { IDBResult, IDBResultError };

export default DbService();
