import './App.css';
import {Button, Intent, Tag, TextArea} from "@blueprintjs/core";
import {Cell, Column, Table2} from "@blueprintjs/table";
import React, {useEffect, useState} from "react";
import init, {CozoDb} from "cozo-lib-wasm";
import {parse} from "ansicolor";

function App() {
    const [db, setDb] = useState(null);
    const [params, setParams] = useState('{\n\n}');
    const [showParams, setShowParams] = useState(false);
    const [queryText, setQueryText] = useState('');
    const [inProgress, setInProgress] = useState(false);
    const [statusMessage, setStatusMessage] = useState('');
    const [errorMessage, setErrorMessage] = useState([]);
    const [queryResults, setQueryResults] = useState(null);

    useEffect(() => {
        init().then(() => {
            let db = CozoDb.new();
            setDb(db);
        })
    }, []);


    const renderCell = (colIdx) => (rowIdx) => <Cell>
        {displayValue(queryResults.rows[rowIdx][colIdx])}
    </Cell>


    function handleKeyDown(e) {
        if (e.key === 'Enter' && e.shiftKey) {
            e.preventDefault();
            e.stopPropagation();
            handleQuery();
        }
        if (e.key === 'Tab' && !e.shiftKey) {
            e.preventDefault();
            e.stopPropagation();
            typeInTextarea('    ');
        }
    }


    function typeInTextarea(newText, el = document.activeElement) {
        const [start, end] = [el.selectionStart, el.selectionEnd];
        el.setRangeText(newText, start, end, 'end');
    }

    function displayValue(v) {
        if (typeof v === 'string') {
            return v
        } else {
            return <span style={{color: "#184A90"}}>{JSON.stringify(v)}</span>
        }
    }

    async function handleQuery() {
        if (!db) {
            setInProgress(false);
            setErrorMessage([]);
            setStatusMessage(['database not ready']);
            setQueryResults(null);
            return;
        }
        const query = queryText.trim();
        if (query) {
            setInProgress(true);
            setErrorMessage([]);
            setStatusMessage('');
            setQueryResults(null);
            try {
                const t0 = performance.now();
                const res_str = db.run(query, params);
                const t1 = performance.now();
                const res = JSON.parse(res_str);
                if (res.ok) {
                    setStatusMessage(`finished with ${res.rows.length} rows in ${(t1 - t0).toFixed(2)}ms`);
                    if (!res.headers) {
                        res.headers = [];
                        if (res.rows.length) {
                            for (let i = 0; i < res.rows[0].length; i++) {
                                res.headers.push('' + i);
                            }
                        }
                    }
                } else {
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
                setErrorMessage(['' + e]);
            } finally {
                setInProgress(false);
            }
        }
    }

    return (
        <div style={{width: "100vw", height: "100vh", display: 'flex', flexDirection: 'column'}}>
            <div style={{padding: 10}}>
                <div style={{display: 'flex'}}>
                    <TextArea
                        autoFocus
                        placeholder="Type query, SHIFT + Enter to run"
                        id="query-box"
                        className="bp4-fill"
                        growVertically={true}
                        large={true}
                        intent={Intent.PRIMARY}
                        onChange={e => setQueryText(e.target.value)}
                        onKeyDown={handleKeyDown}
                        value={queryText}
                    />
                    {showParams && <TextArea
                        id="params-box"
                        style={{marginLeft: 5}}
                        placeholder="Your params (a JSON map)"
                        large={true}
                        onChange={e => setParams(e.target.value)}
                        onKeyDown={handleKeyDown}
                        value={params}
                    />}
                </div>
                <div/>
                <div style={{paddingTop: 10, display: 'flex', flexDirection: 'row'}}>
                    <Button text={db ? "Run script" : "Loading Cozo library, please wait ..."}
                            onClick={() => handleQuery()}
                            disabled={!db || inProgress}
                            intent={Intent.PRIMARY}
                    />
                    &nbsp;
                    <Button onClick={() => {
                        setShowParams(!showParams)
                    }}>{showParams ? 'Hide' : 'Show'} params</Button>
                    <div style={{marginLeft: 10, marginTop: 5}}>
                        {statusMessage ? <Tag intent={errorMessage.length ? Intent.DANGER : Intent.SUCCESS} minimal>
                            {statusMessage}
                        </Tag> : null}
                    </div>
                </div>
            </div>
            {errorMessage.length ? <pre id="error-message">
                {errorMessage.map((item, id) => {
                    if (typeof item === 'string') {
                        return <span key={id}>{item}</span>
                    } else {
                        let styles = {};
                        if (item.css) {
                            styles.color = item.css.replace('color:', '').replace(';', '');
                        }
                        return <span key={id} style={styles}>{item.text}</span>
                    }
                })}
            </pre> : null}
            {queryResults ? (queryResults.rows && queryResults.headers ?
                <Table2
                    numRows={queryResults.rows.length}
                >
                    {queryResults.headers.map((n, idx) => <Column
                        name={n}
                        key={idx}
                        cellRenderer={renderCell(idx)}
                    />)}
                </Table2> : null) : null}
            {!(queryResults || errorMessage.length) && <div id="welcome">
                <p>
                    This is the demo page for Cozo running in your browser as
                    an <a href="https://webassembly.org/">Web assembly</a> module.
                </p>
                <p>
                    All computation is done within your browser. There is no backend, nor any outgoing requests.
                </p>
                <p>
                    Please refer to the <a href="https://github.com/cozodb/cozo/">project homepage</a> for
                    more information about the Cozo database.
                </p>
                <h2>Not sure what to run? Click/touch <a onClick={() => {
                    setQueryText(`love[loving, loved] <- [['alice', 'eve'],
                        ['bob', 'alice'],
                        ['eve', 'alice'],
                        ['eve', 'bob'],
                        ['eve', 'charlie'],
                        ['charlie', 'eve'],
                        ['david', 'george'],
                        ['george', 'george']]

?[person, page_rank] <~ PageRank(love[])`)
                }}>HERE</a> ...</h2>
                <p>
                    ... and run the script, to compute the <a href="https://www.wikiwand.com/en/PageRank">PageRank</a> of
                    a hypothetical love triangle.
                </p>
            </div>}
        </div>
    );
}

export default App;
