/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

import './App.css';
import {
    Button,
    Checkbox,
    Classes,
    Dialog,
    FileInput,
    InputGroup,
    Intent,
    Tag,
    TextArea,
    Toaster
} from "@blueprintjs/core";
import {Cell, Column, Table2} from "@blueprintjs/table";
import React, {useEffect, useState} from "react";
import init, {CozoDb} from "cozo-lib-wasm";
import {parse} from "ansicolor";
import {saveAs} from 'file-saver';


function App() {
    const [db, setDb] = useState(null);
    const [params, setParams] = useState('{}');
    const [showParams, setShowParams] = useState(false);
    const [queryText, setQueryText] = useState('');
    const [inProgress, setInProgress] = useState(false);
    const [statusMessage, setStatusMessage] = useState('');
    const [errorMessage, setErrorMessage] = useState([]);
    const [queryResults, setQueryResults] = useState(null);
    const [queryId, setQueryId] = useState(0);

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

    function handleQuery() {
        if (!db || inProgress) {
            setInProgress(false);
            setErrorMessage([]);
            setStatusMessage(['database not ready']);
            setQueryResults(null);
            return;
        }
        setQueryId(queryId + 1);
        const query = queryText.trim();
        if (query) {
            setInProgress(true);
            setErrorMessage([]);
            setStatusMessage('');
            setQueryResults(null);
            requestAnimationFrame(() => {
                setTimeout(() => {
                    try {
                        const t0 = performance.now();
                        const res_str = db.run(query, params);
                        const t1 = performance.now();
                        const res = JSON.parse(res_str);
                        if (res.ok) {
                            setStatusMessage(`finished with ${res.rows.length} rows in ${(t1 - t0).toFixed(1)}ms`);
                            if (!res.headers) {
                                res.headers = [];
                                if (res.rows.length) {
                                    for (let i = 0; i < res.rows[0].length; i++) {
                                        res.headers.push('' + i);
                                    }
                                }
                            }
                        } else {
                            console.error('Query failed', res);
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
                }, 0)
            })
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
                    <Button
                        icon="play"
                        text={db ? (inProgress ? "Query is running" : "Run script") : "Loading WASM ..."}
                        onClick={() => handleQuery()}
                        disabled={!db || inProgress}
                        intent={Intent.PRIMARY}
                    />
                    &nbsp;
                    <div style={{marginLeft: 10, marginTop: 5}}>
                        {statusMessage ? <Tag intent={errorMessage.length ? Intent.DANGER : Intent.SUCCESS} minimal>
                            {statusMessage}
                        </Tag> : null}
                    </div>
                    <div style={{flex: 1}}/>
                    <Export db={db}/>
                    <ImportUrl db={db}/>
                    <ImportFile db={db}/>
                    <Button icon="properties" style={{marginLeft: 5}} onClick={() => {
                        setShowParams(!showParams)
                    }}>Params</Button>
                </div>
            </div>
            {errorMessage.length ? <pre id="error-message">
                {errorMessage.map((item, id) => {
                    if (typeof item === 'string') {
                        return <span key={id}>{item}</span>
                    } else {
                        let styles = {};
                        if (item.css) {
                            for (let pair of item.css.split(';')) {
                                pair = pair.trim();
                                if (pair) {
                                    const [k, v] = pair.split(':');
                                    if (k.trim() === 'font-weight') {
                                        styles['fontWeight'] = v.trim()
                                    } else {
                                        styles[k.trim()] = v.trim();
                                    }
                                }
                            }
                        }
                        return <span key={id} style={styles}>{item.text}</span>
                    }
                })}
            </pre> : null}
            {queryResults ? (queryResults.rows && queryResults.headers ?
                <Table2
                    cellRendererDependencies={queryResults.rows}
                    numRows={queryResults.rows.length}
                >
                    {queryResults.headers.map((n, idx) => <Column
                        name={n}
                        key={idx}
                        cellRenderer={renderCell(idx)}
                    />)}
                </Table2> : null) : null}
            {!(queryResults || errorMessage.length || inProgress) && <div id="welcome">
                <p>
                    This is the demo page for Cozo running in your browser as
                    a <a href="https://webassembly.org/">Web assembly</a> module.
                </p>
                <p>
                    All computation is done within your browser. There is no backend, nor any outgoing requests.
                </p>
                <p>
                    Please refer to the <a href="https://www.cozodb.org">project homepage</a> for
                    more information about the Cozo database.
                </p>
                <h2>Not sure what to run?</h2>
                <p>
                    <a onClick={() => {
                    setQueryText(`parent[] <- [['joseph', 'jakob'], 
             ['jakob', 'issac'], 
             ['issac', 'abraham']]
grandparent[gcld, gp] := parent[gcld, p], parent[p, gp]
?[who] := grandparent[who, 'abraham']`)
                }}>Here</a> is a classical example recursive example.
                </p>
                <p>
                    The <a href="https://docs.cozodb.org/en/latest/tutorial.html">tutorial</a> contains many more examples.
                </p>
            </div>}
        </div>
    );
}

function ImportUrl({db}) {
    const [open, setOpen] = useState(false);
    const [url, setUrl] = useState('');

    function handleClose() {
        setOpen(false)
    }

    async function handleImport() {
        try {
            let resp = await fetch(url);
            let content = await resp.text();
            const res = JSON.parse(db.import_relations(content));
            if (res.ok) {
                AppToaster.show({message: "Imported", intent: Intent.SUCCESS})
                handleClose()
            } else {
                AppToaster.show({message: res.message, intent: Intent.DANGER})
            }
        } catch (e) {
            AppToaster.show({message: '' + e, intent: Intent.DANGER})
        }


    }

    return <>
        <Button icon="import" style={{marginLeft: 5}} onClick={() => {
            setUrl('');
            setOpen(true)
        }}>
            URL
        </Button>
        <Dialog isOpen={open} title="Import data from URL" onClose={handleClose}>
            <div className={Classes.DIALOG_BODY}>
                <InputGroup
                    fill
                    placeholder="Enter the file URL"
                    value={url}
                    onChange={e => setUrl(e.target.value)}
                />
            </div>

            <div className={Classes.DIALOG_FOOTER}>
                <div className={Classes.DIALOG_FOOTER_ACTIONS}>
                    <Button onClick={handleClose}>Cancel</Button>
                    <Button intent={Intent.PRIMARY} disabled={!url} onClick={handleImport}>Import</Button>
                </div>
            </div>
        </Dialog>
    </>
}

function ImportFile({db}) {
    const [open, setOpen] = useState(false);
    const [file, setFile] = useState(null);

    function handleClose() {
        setOpen(false)
    }

    async function handleImport() {
        try {
            let content = await file.text();
            const res = JSON.parse(db.import_relations(content));
            if (res.ok) {
                AppToaster.show({message: "Imported", intent: Intent.SUCCESS})
                handleClose()
            } else {
                AppToaster.show({message: res.message, intent: Intent.DANGER})
            }
        } catch (e) {
            AppToaster.show({message: '' + e, intent: Intent.DANGER})
        }


    }

    return <>
        <Button icon="import" style={{marginLeft: 5}} onClick={() => setOpen(true)}>
            File
        </Button>
        <Dialog isOpen={open} title="Import data from local file" onClose={handleClose}>
            <div className={Classes.DIALOG_BODY}>
                <FileInput fill text={(file && file.name) || 'Choose file ...'} onInputChange={(e) => {
                    setFile(e.target.files[0]);
                }}/>
            </div>

            <div className={Classes.DIALOG_FOOTER}>
                <div className={Classes.DIALOG_FOOTER_ACTIONS}>
                    <Button onClick={handleClose}>Cancel</Button>
                    <Button intent={Intent.PRIMARY} disabled={!file} onClick={handleImport}>Import</Button>
                </div>
            </div>
        </Dialog>
    </>
}

function Export({db}) {
    const [rels, setRels] = useState([]);
    const [selected, setSelected] = useState([]);

    function toggle() {
        if (rels.length) {
            setRels([])
        } else {
            const relations = JSON.parse(db.run('::relations', '')).rows;
            if (!relations.length) {
                AppToaster.show({message: 'No stored relations to export', intent: Intent.WARNING})
                return;
            }

            setSelected([]);
            setRels(relations)
        }
    }

    function handleClose() {
        setRels([])
    }

    function handleExport() {
        const res = JSON.parse(db.export_relations(JSON.stringify({relations: selected})));
        if (res.ok) {
            const blob = new Blob([JSON.stringify(res.data)], {type: "text/plain;charset=utf-8"});
            saveAs(blob, "export.json");
            AppToaster.show({message: "Exported", intent: Intent.SUCCESS})
            handleClose()
        } else {
            AppToaster.show({message: res.message, intent: Intent.DANGER})
        }
    }

    return <>
        <Button icon="export" style={{marginLeft: 5}} onClick={toggle}>
            Export
        </Button>
        <Dialog isOpen={!!rels.length} onClose={handleClose} title="Export">
            <div className={Classes.DIALOG_BODY}>
                <p>Choose stored relations to export:</p>
                {rels.map((row) => <Checkbox
                    key={row[0]} label={row[0]} checked={selected.includes(row[0])}
                    onChange={() => {
                        if (selected.includes(row[0])) {
                            setSelected(selected.filter(n => n !== row[0]))
                        } else {
                            setSelected([...selected, row[0]])
                        }
                    }}/>)}
            </div>
            <div className={Classes.DIALOG_FOOTER}>
                <div className={Classes.DIALOG_FOOTER_ACTIONS}>
                    <Button onClick={handleClose}>Cancel</Button>
                    <Button intent={Intent.PRIMARY} disabled={!selected.length} onClick={handleExport}>Export</Button>
                </div>
            </div>
        </Dialog>
    </>
}

const AppToaster = Toaster.create({position: 'top-right'});


export default App;
