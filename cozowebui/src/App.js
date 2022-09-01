import {Button, Intent, Tag, TextArea} from "@blueprintjs/core";
import React, {useState} from 'react';
import './App.css';
import {Cell, Column, Table2} from "@blueprintjs/table";

function App() {
    const [queryText, setQueryText] = useState('');
    const [inProgress, setInProgress] = useState(false);
    const [statusMessage, setStatusMessage] = useState('');
    const [errorMessage, setErrorMessage] = useState('');
    const [queryResults, setQueryResults] = useState(null);


    function handleKeyDown(e) {
        if (e.key === 'Enter' && e.shiftKey) {
            e.preventDefault();
            e.stopPropagation();
            handleQuery();
        }
    }

    const renderCell = (colIdx) => (rowIdx) => <Cell>
        {displayValue(queryResults.rows[rowIdx][colIdx])}
    </Cell>

    function displayValue(v) {
        if (v.constructor === Array) {
            return '[' + v.map(el => displayValue(el)).join(', ') + ']'
        } else {
            return '' + v
        }
    }

    async function handleQuery() {
        const query = queryText.trim();
        if (query) {
            let started = performance.now();
            setInProgress(true);
            setErrorMessage('');
            setStatusMessage('');
            setQueryResults(null);
            try {
                let url = '/text-query';
                if (!process.env.NODE_ENV || process.env.NODE_ENV === 'development') {
                    url = 'http://127.0.0.1:9070' + url;
                }

                const response = await fetch(url, {
                    method: 'POST',
                    body: query
                });

                if (!response.ok) {
                    throw await response.text();
                }
                let res = await response.json();
                if (res.rows) {
                    setStatusMessage(`finished with ${res.rows.length} rows in ${res.time_taken}ms`);
                    if (!res.headers) {
                        res.headers = [];
                        if (res.rows.length) {
                            for (let i = 0; i < res.rows[0].length; i++) {
                                res.headers.push('?' + i);
                            }
                        }
                    }
                } else {
                    setStatusMessage(`finished in ${res.time_taken}ms`);
                }
                setQueryResults(res);
            } catch (e) {
                let time = Math.round(performance.now() - started);
                setStatusMessage(`finished with error in ${time}ms`);
                setErrorMessage('' + e);
            } finally {
                setInProgress(false);
            }
        }
    }

    return (
        <div style={{width: "100vw", height: "100vh", display: 'flex', flexDirection: 'column'}}>
            <div style={{padding: 10}}>
                <TextArea
                    placeholder="Input CozoScript here, SHIFT + Enter to run"
                    id="query-box"
                    className="bp4-fill"
                    growVertically={true}
                    large={true}
                    intent={Intent.PRIMARY}
                    onChange={e => setQueryText(e.target.value)}
                    onKeyDown={handleKeyDown}
                    value={queryText}
                />
                <div/>
                <div style={{paddingTop: 10, display: 'flex', flexDirection: 'row'}}>
                    <Button text="Run" onClick={handleQuery}
                            disabled={inProgress}/>
                    <div style={{marginLeft: 10, marginTop: 5}}>
                        {statusMessage ? <Tag intent={errorMessage ? Intent.DANGER : Intent.SUCCESS} minimal>
                            {statusMessage}
                        </Tag> : null}
                    </div>
                </div>
            </div>
            {errorMessage ? <pre id="error-message">{errorMessage}</pre> : null}
            {queryResults ? (queryResults.rows && queryResults.headers ?
                <Table2
                    numRows={queryResults.rows.length}
                >
                    {queryResults.headers.map((n, idx) => <Column
                        name={n.replace(/^\?/, '')}
                        key={idx}
                        cellRenderer={renderCell(idx)}
                    />)}
                </Table2> :
                <pre id="other-results">{JSON.stringify(queryResults, null, 2)}</pre>) : null}
        </div>
    );
}

export default App;
