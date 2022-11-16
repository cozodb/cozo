import {Button, Intent, Tag, TextArea} from "@blueprintjs/core";
import React, {useState} from 'react';
import './App.css';
import {Cell, Column, Table2} from "@blueprintjs/table";

let db;

import('./setup')
    .catch(e => console.error("Error importing `cozo-lib-wasm`:", e))
    .then(mod => {
        console.log(mod)
        // db = new mod.CozoDb();
        // console.log(db.run('xxxx'))
    })

function App() {
    const [params, setParams] = useState('');
    const [queryText, setQueryText] = useState('');
    const [inProgress, setInProgress] = useState(false);
    const [statusMessage, setStatusMessage] = useState('');
    const [errorMessage, setErrorMessage] = useState('');
    const [queryResults, setQueryResults] = useState(null);

    function handleKeyDown(e) {
        if (e.key === 'Enter' && e.shiftKey) {
            e.preventDefault();
            e.stopPropagation();
            handleQuery('script');
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

    const renderCell = (colIdx) => (rowIdx) => <Cell>
        {displayValue(queryResults.rows[rowIdx][colIdx])}
    </Cell>

    function displayValue(v) {
        if (typeof v === 'string') {
            return v
        } else {
            return <span style={{color: "#184A90"}}>{JSON.stringify(v)}</span>
        }
    }

    async function handleQuery(type) {
        if (!db) {
            setErrorMessage('Cannot open database');
            return;
        }
        const query = queryText.trim();
        if (query) {
            let started = performance.now();
            setInProgress(true);
            setErrorMessage('');
            setStatusMessage('');
            setQueryResults(null);
            try {
                let res = JSON.parse(db.run(query, JSON.parse(params.trim() || '{}')));
                if (res.ok) {
                    setStatusMessage(`finished with ${res.rows.length} rows`);
                    if (!res.headers) {
                        res.headers = [];
                        if (res.rows.length) {
                            for (let i = 0; i < res.rows[0].length; i++) {
                                res.headers.push('' + i);
                            }
                        }
                    }
                } else {
                    setStatusMessage(`Failed to execute query`);
                    setErrorMessage(res.display || res.message);
                }
                setQueryResults(res);
            } catch (e) {
                setStatusMessage(`Encountered problem when executing query`);
                setErrorMessage('' + e);
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
                        placeholder="Type query here, SHIFT + Enter to run as script"
                        id="query-box"
                        className="bp4-fill"
                        growVertically={true}
                        large={true}
                        intent={Intent.PRIMARY}
                        onChange={e => setQueryText(e.target.value)}
                        onKeyDown={handleKeyDown}
                        value={queryText}
                    />
                    <TextArea
                        id="params-box"
                        style={{marginLeft: 5}}
                        placeholder="Type your params here (a JSON map)"
                        large={true}
                        onChange={e => setParams(e.target.value)}
                        onKeyDown={handleKeyDown}
                        value={params}
                    />
                </div>
                <div/>
                <div style={{paddingTop: 10, display: 'flex', flexDirection: 'row'}}>
                    <Button text="Run script" onClick={() => handleQuery('script')}
                            disabled={inProgress}/>
                    {/*<Button text="Convert script to JSON" onClick={() => handleQuery('convert')}*/}
                    {/*        disabled={inProgress} style={{marginLeft: 5}}/>*/}
                    {/*<Button text="Run JSON" onClick={() => handleQuery('json')}*/}
                    {/*        disabled={inProgress} style={{marginLeft: 5}}/>*/}

                    <div style={{marginLeft: 10, marginTop: 5}}>
                        {statusMessage ? <Tag intent={errorMessage ? Intent.DANGER : Intent.SUCCESS} minimal>
                            {statusMessage}
                        </Tag> : null}
                    </div>
                </div>
            </div>
            {errorMessage ? <pre id="error-message" dangerouslySetInnerHTML={{__html: errorMessage}}></pre> : null}
            {queryResults ? (queryResults.rows && queryResults.headers ?
                <Table2
                    numRows={queryResults.rows.length}
                >
                    {queryResults.headers.map((n, idx) => <Column
                        name={n}
                        key={idx}
                        cellRenderer={renderCell(idx)}
                    />)}
                </Table2> :
                <pre id="other-results">{JSON.stringify(queryResults, null, 2)}</pre>) : null}
        </div>
    );
}

export default App;
