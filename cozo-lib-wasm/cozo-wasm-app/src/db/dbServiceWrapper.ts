import { wrap, proxy } from "comlink";

const worker = new Worker(new URL("./dbWorker.js", import.meta.url), {
  type: "module",
});

// Create a Comlink proxy for the worker
const dbServiceProxy = wrap<typeof import("./dbWorker").api>(worker);

function DbServiceWrapper() {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  let writesCount = 0;
  const init = async () => {
    await dbServiceProxy.init();

    // Sync writesCount between worker to app
    worker.onmessage = (event) => {
      const { type, value } = event.data;
      if (type === "writesCountUpdate") {
        writesCount = value;
      }
    };
  };

  const executePutCommand = async (tableName: string, array: any[][]) =>
    dbServiceProxy.executePutCommand(tableName, array);

  const executeBatchPutCommand = async (
    tableName: string,
    array: any[],
    batchSize: number,
    onProgress?: (count: number) => void
  ) =>
    dbServiceProxy.executeBatchPutCommand(
      tableName,
      array,
      batchSize,
      proxy(onProgress)
    );

  const runCommand = async (command: string) =>
    dbServiceProxy.runCommand(command);

  const executeGetCommand = async (
    tableName: string,
    conditionArr?: string[],
    keys?: string[]
  ) => dbServiceProxy.executeGetCommand(tableName, conditionArr, keys);

  return {
    init,
    executePutCommand,
    executeBatchPutCommand,
    runCommand,
    executeGetCommand,
  };
}

export default DbServiceWrapper();
