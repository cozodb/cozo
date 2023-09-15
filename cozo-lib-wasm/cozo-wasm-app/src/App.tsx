/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import "./App.css";
import {
  Button,
  Intent,
  Tag,
  TextArea,
  Menu,
  MenuItem,
  Divider,
  Popover,
  ButtonGroup,
} from "@blueprintjs/core";
import { Cell, Column, Table2 } from "@blueprintjs/table";
import React, { useEffect, useState } from "react";
import { parse } from "ansicolor";

import cozoDb from "./db/dbServiceWrapper";

const diffMs = (t0: number, t1: number) => `${(t1 - t0).toFixed(1)}ms`;

const exampleQueries = {
  "Coins (filter)": `?[id, symbol, name] := *coin{id, symbol, name}, starts_with(symbol, 'btc') :order -symbol`,
  "Market (join/calc)": `?[symbol, current_price, high_24h, low_24h, ath] := *coin{id, symbol, name}, *market{id: id, current_price, high_24h, low_24h}, ath=high_24h-low_24h :order -current_price`,
  "Stats (aggregations)": `?[min(current_price), max(current_price), sum(market_cap)] := *market{market_cap, current_price}`,
};

function useLog(): [
  string[],
  (messages: string[] | string) => void,
  (message: string) => void,
  () => void
] {
  const [logMessages, setLogMessages] = useState<string[]>([]);

  const appendLog = (messages: string[] | string) => {
    const items = messages instanceof Array ? messages : [messages];
    setLogMessages((prev) => [...prev, ...items]);
  };

  const updateLastLog = (message: string) =>
    setLogMessages((prev) => [...prev.slice(0, prev.length - 1), message]);

  const clearLog = () => setLogMessages([]);

  return [logMessages, appendLog, updateLastLog, clearLog];
}

function App() {
  const [isCozoInitialized, setIsCozoInitialized] = useState(false);
  const [inProgress, setInProgress] = useState(false);

  const [queryText, setQueryText] = useState("");
  const [statusMessage, setStatusMessage] = useState("");
  const [errorMessage, setErrorMessage] = useState([]);
  const [queryResults, setQueryResults] = useState(null);

  const [logs, appendLog, updateLastLog, clearLog] = useLog();

  useEffect(() => {
    cozoDb.init().then(() => setIsCozoInitialized(true));
  }, []);

  const importCoingecko = async () => {
    const t1 = performance.now();
    clearLog();
    appendLog("Fetch USD markets...");

    const vsCurrency = "usd";

    const markets = await (
      await fetch(
        `https://api.coingecko.com/api/v3/coins/markets?vs_currency=${vsCurrency}&order=market_cap_desc&per_page=250&page=1&sparkline=false&locale=en`
      )
    ).json();
    const marketsNormalized = markets.map((item) => ({
      ...item,
      vs_currency: vsCurrency,
      last_updated: new Date(item.last_updated).getTime(),
    }));

    appendLog(`⏳ Import 0/${markets.length} markets into db...`);

    await cozoDb.executeBatchPutCommand(
      "market",
      marketsNormalized,
      1,
      (counter) => {
        updateLastLog(
          `⏳ Import ${counter}/${markets.length} markets into db...`
        );
      }
    );
    const marketIds = markets.map((item) => item.id);

    updateLastLog(
      `☑️ Imported ${markets.length} markets in ${diffMs(
        t1,
        performance.now()
      )}.`
    );

    const t2 = performance.now();

    appendLog("Fetch coins from Coingecko...");
    const coins = (
      await (await fetch("https://api.coingecko.com/api/v3/coins/list")).json()
    ).filter((item) => marketIds.includes(item.id));

    appendLog(`⏳ Import 0/${coins.length} coins into db...`);
    await cozoDb.executeBatchPutCommand("coin", coins, 100, (counter) =>
      updateLastLog(`⏳ Import ${counter}/${coins.length} coins into db...`)
    );
    updateLastLog(
      `☑️ Imported ${coins.length} coins in ${diffMs(t2, performance.now())}.`
    );

    appendLog(["", `🎉 All done in ${diffMs(t1, performance.now())}`]);
  };

  const renderCell = (colIdx) => (rowIdx) =>
    <Cell>{queryResults.rows[rowIdx][colIdx]}</Cell>;

  function handleKeyDown(e) {
    if (e.key === "Enter" && e.shiftKey) {
      e.preventDefault();
      e.stopPropagation();
      handleQuery();
    }
    if (e.key === "Tab" && !e.shiftKey) {
      e.preventDefault();
      e.stopPropagation();
      typeInTextarea("    ");
    }
  }

  function typeInTextarea(newText, el = document.activeElement) {
    const [start, end] = [el.selectionStart, el.selectionEnd];
    el.setRangeText(newText, start, end, "end");
  }

  function handleQuery(inputQuery = queryText) {
    if (inProgress) {
      //!db ||
      setInProgress(false);
      setErrorMessage([]);
      setStatusMessage("database not ready");
      setQueryResults(null);
      return;
    }
    const query = inputQuery.trim();
    if (query) {
      setInProgress(true);
      setErrorMessage([]);
      setStatusMessage("");
      setQueryResults(null);
      requestAnimationFrame(() => {
        setTimeout(async () => {
          try {
            const t0 = performance.now();
            const t1 = performance.now();
            // const res = JSON.parse(res_str);
            const res = await cozoDb.runCommand(query);
            console.log("query results", res);
            if (res.ok) {
              setStatusMessage(
                `finished with ${res.rows.length} rows in ${(t1 - t0).toFixed(
                  1
                )}ms`
              );
              if (!res.headers) {
                res.headers = [];
                if (res.rows.length) {
                  for (let i = 0; i < res.rows[0].length; i++) {
                    res.headers.push("" + i);
                  }
                }
              }
            } else {
              console.error("Query failed", res);
              setStatusMessage(`finished with errors`);
              if (res.display) {
                const messages = parse(res.display);
                setErrorMessage(messages.spans);
              } else {
                setErrorMessage([res.message]);
              }
            }
            setQueryResults(res);
          } catch (e) {
            setStatusMessage(`query failed`);
            setErrorMessage(["" + e]);
          } finally {
            setInProgress(false);
          }
        }, 0);
      });
    }
  }

  const execQuery = (query) => {
    setQueryText(query);
    handleQuery(query);
  };

  const examplesMenu = (
    <Menu>
      {Object.keys(exampleQueries).map((k, index) => (
        <MenuItem
          key={`example_${index}`}
          text={k}
          onClick={() => execQuery(exampleQueries[k])}
        />
      ))}
    </Menu>
  );

  return (
    <main>
      <div className="query-panel">
        <TextArea
          autoFocus
          placeholder="Type query, SHIFT + Enter to run"
          id="query-box"
          className="bp4-fill"
          growVertically={true}
          large={true}
          intent={Intent.PRIMARY}
          onChange={(e) => setQueryText(e.target.value)}
          onKeyDown={handleKeyDown}
          value={queryText}
        />
        <div />
        <div className="button-panel">
          <Button
            icon="play"
            text={
              isCozoInitialized
                ? inProgress
                  ? "Query is running"
                  : "Run script"
                : "Loading WASM ..."
            }
            onClick={() => handleQuery()}
            disabled={inProgress}
            // !db ||
            intent={Intent.PRIMARY}
          />
          <div className="query-info">
            {statusMessage ? (
              <Tag
                intent={errorMessage.length ? Intent.DANGER : Intent.SUCCESS}
                minimal
              >
                {statusMessage}
              </Tag>
            ) : null}
          </div>
          {isCozoInitialized && (
            <div className="action-buttons">
              <ButtonGroup>
                <Button
                  icon="import"
                  intent={Intent.WARNING}
                  onClick={async () => importCoingecko()}
                >
                  Import sample data
                </Button>
                <Divider />
                <Popover content={examplesMenu} fill={true} placement="bottom">
                  <Button
                    alignText="left"
                    icon="applications"
                    text="Execute example query..."
                  />
                </Popover>
              </ButtonGroup>
            </div>
          )}
        </div>
      </div>
      {logs.length ? (
        <pre id="import-results">
          {logs.map((item, index) => (
            <div key={`log-${index}`}>{item}</div>
          ))}{" "}
        </pre>
      ) : null}
      {errorMessage.length ? (
        <pre id="error-message">
          {errorMessage.map((item, id) => {
            if (typeof item === "string") {
              return <span key={id}>{item}</span>;
            } else {
              let styles = {};
              if (item.css) {
                for (let pair of item.css.split(";")) {
                  pair = pair.trim();
                  if (pair) {
                    const [k, v] = pair.split(":");
                    if (k.trim() === "font-weight") {
                      styles["fontWeight"] = v.trim();
                    } else {
                      styles[k.trim()] = v.trim();
                    }
                  }
                }
              }
              return (
                <span key={id} style={styles}>
                  {item.text}
                </span>
              );
            }
          })}
        </pre>
      ) : null}
      {queryResults ? (
        queryResults.rows && queryResults.headers ? (
          <Table2
            cellRendererDependencies={queryResults.rows}
            numRows={queryResults.rows.length}
          >
            {queryResults.headers.map((n, idx) => (
              <Column name={n} key={idx} cellRenderer={renderCell(idx)} />
            ))}
          </Table2>
        ) : null
      ) : null}
      {!(queryResults || errorMessage.length || inProgress) && (
        <div id="welcome">
          <p>
            This is a example of usage of hacked version of Cozo WASM with{" "}
            <b>permanent</b> "Mem" storage support.
          </p>
          <p>
            Cozo doesn't support async operations, so it's impossible to handle
            writes to IndexedDb in WASM.
          </p>
          <p>
            This example shows how to manage write operations outside of WASM
            module.
          </p>
          <ol>
            <li>
              Press "Import sample data" to import sample data from Coingecko
              API and store into DB.
            </li>
            <li>
              Press "Execute example query..." to quict try of one of the
              example queries.
            </li>
          </ol>
          <br />
          <p>
            Please refer to the{" "}
            <a href="https://www.cozodb.org">project homepage</a> for more
            information about the Cozo database.
          </p>
        </div>
      )}
    </main>
  );
}

export default App;
