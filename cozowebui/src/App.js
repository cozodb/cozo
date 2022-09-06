import {Button, InputGroup, Intent, Tag, TextArea, Toaster} from "@blueprintjs/core";
import React, {useState} from 'react';
import './App.css';
import {Cell, Column, Table2} from "@blueprintjs/table";

function App() {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [params, setParams] = useState('');
    const [validating, setValidating] = useState(false);
    const [usernameInput, setUsernameInput] = useState('');
    const [passwordInput, setPasswordInput] = useState('');
    const [queryText, setQueryText] = useState('');
    const [inProgress, setInProgress] = useState(false);
    const [statusMessage, setStatusMessage] = useState('');
    const [errorMessage, setErrorMessage] = useState('');
    const [queryResults, setQueryResults] = useState(null);


    async function validateCredentials() {
        if (validating || !usernameInput || !passwordInput) {
            return
        }
        let url = '/text-query';

        if (!process.env.NODE_ENV || process.env.NODE_ENV === 'development') {
            url = 'http://127.0.0.1:9070' + url;
        }

        try {
            setValidating(true);
            const response = await fetch(url, {
                method: 'POST',
                body: JSON.stringify({script: '?[ok] := ok <- true;', params: {}}),
                headers: new Headers({
                    'content-type': 'application/json',
                    'x-cozo-username': usernameInput,
                    'x-cozo-password': passwordInput
                }),
            });
            if (!response.ok) {
                throw Error(response);
            }
            let json = await response.json();
            if (json.rows[0][0]) {
                setUsername(usernameInput);
                setPassword(passwordInput);
            } else {
                throw Error(json)
            }
        } catch (e) {
            Toaster.create().show({message: 'Cannot authenticate', intent: Intent.DANGER});
            console.error(e);
        } finally {
            setValidating(false);
        }
    }

    function handleKeyDown(e) {
        if (e.key === 'Enter' && e.shiftKey) {
            e.preventDefault();
            e.stopPropagation();
            handleQuery('script');
        }
    }

    const renderCell = (colIdx) => (rowIdx) => <Cell>
        {displayValue(queryResults.rows[rowIdx][colIdx])}
    </Cell>

    function displayValue(v) {
        if (v instanceof Object) {
            return JSON.stringify(v)
        } else {
            return v
        }
    }

    async function handleQuery(type) {
        const query = queryText.trim();
        if (query) {
            let started = performance.now();
            setInProgress(true);
            setErrorMessage('');
            setStatusMessage('');
            setQueryResults(null);
            try {
                let url;
                if (type === 'json') {
                    url = '/json-query'
                } else if (type === 'convert') {
                    url = '/script-to-json'
                } else {
                    url = '/text-query';
                }
                if (!process.env.NODE_ENV || process.env.NODE_ENV === 'development') {
                    url = 'http://127.0.0.1:9070' + url;
                }

                let response;
                if (type === 'json') {
                    response = await fetch(url, {
                        method: 'POST',
                        body: JSON.stringify(query),
                        headers: new Headers({
                            'content-type': 'application/json',
                            'x-cozo-username': username,
                            'x-cozo-password': password
                        }),
                    });
                } else {
                    response = await fetch(url, {
                        method: 'POST',
                        body: JSON.stringify({script: query, params: JSON.parse(params.trim() || '{}')}),
                        headers: new Headers({
                            'content-type': 'application/json',
                            'x-cozo-username': username,
                            'x-cozo-password': password
                        }),
                    });
                }

                if (!response.ok) {
                    throw await response.text();
                }
                let res = await response.json();
                if (res.rows) {
                    setStatusMessage(`finished with ${res.rows.length} rows in ${res.time_taken || 0}ms`);
                    if (!res.headers) {
                        res.headers = [];
                        if (res.rows.length) {
                            for (let i = 0; i < res.rows[0].length; i++) {
                                res.headers.push('' + i);
                            }
                        }
                    }
                } else {
                    setStatusMessage(`finished in ${res.time_taken || 0}ms`);
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

    if (!(username && password)) {
        return <div
            style={{display: 'flex', alignItems: 'center', justifyContent: 'center', width: '100vw', height: '100vh'}}>
            <div style={{
                width: 250,
                height: 100,
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between'
            }}>
                <InputGroup placeholder="Username" value={usernameInput}
                            onChange={v => setUsernameInput(v.target.value)}
                            autoFocus
                />
                <InputGroup placeholder="Password" value={passwordInput}
                            onChange={v => setPasswordInput(v.target.value)}
                            onKeyDown={e => {
                                if (e.key === 'Enter') {
                                    validateCredentials();
                                }
                            }}
                            type="password"/>
                <Button disabled={validating} intent={Intent.PRIMARY}
                        onClick={validateCredentials}>Authenticate</Button>
            </div>
        </div>
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
