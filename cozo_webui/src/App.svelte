<script>
    let queryText = '';

    let queryResults = null;
    let started;
    let inProgress = false;
    let errorMessage = '';
    let statusMessage = '';

    import {Button, DataTable, InlineLoading, TextArea} from "carbon-components-svelte";
    import {DirectionStraightRight} from "carbon-icons-svelte";
    import {onMount} from "svelte";

    async function handleQuery() {
        const query = queryText.trim();
        if (query) {
            inProgress = true;
            started = performance.now();
            errorMessage = '';
            statusMessage = '';
            queryResults = null;
            try {
                const response = await fetch('/text-query', {
                    method: 'POST',
                    body: query
                });

                if (!response.ok) {
                    throw await response.text();
                }
                let res = await response.json();
                statusMessage = `finished in ${res.time_taken}ms`

                if (res.rows) {
                    if (!res.headers) {
                        res.headers = [];
                        if (res.rows.length) {
                            for (let i = 0; i < res.rows[0].length; i++) {
                                res.headers.push('?' + i);
                            }
                        }
                    }
                    const headers = res.headers.map((h) => ({key: h, value: h}));
                    const rows = res.rows.map((v, idx) => {
                        let ret = {};
                        ret.id = idx;
                        for (let i = 0; i < v.length; i++) {
                            ret[headers[i].key] = v[i];
                        }
                        return ret;
                    });
                    queryResults = {rows, headers}
                } else {
                    queryResults = res;
                }
            } catch (e) {
                let time = Math.round(performance.now() - started);
                statusMessage = `finished in ${time}ms`
                errorMessage = '' + e;
            } finally {
                inProgress = false;
            }
        }
        document.getElementById("query-area").focus();
    }
</script>

<main>
    <div id="main">
        <div id="upper">
                <TextArea bind:value={queryText} rows={10}
                          id="query-area"></TextArea>
            <div style="width: 100%; display: flex; align-items: stretch; justify-content: center; flex-direction: row; padding-bottom: 10px">
                <div style="flex: 1">
                    {#if inProgress}
                        <InlineLoading status="active" description="In progress..."/>
                    {:else if statusMessage}
                        <InlineLoading status={errorMessage ? 'error' : 'finished'} description={statusMessage}/>
                    {/if}
                </div>
                <div>
                    <Button size="small" icon={DirectionStraightRight} on:click={handleQuery} disabled={inProgress}>
                        Query
                    </Button>
                </div>
            </div>

        </div>
        {#if errorMessage}
            <pre id="error-message">{errorMessage}</pre>
        {/if}
        {#if queryResults}
            {#if queryResults.headers && queryResults.rows}
                <DataTable headers={queryResults.headers} rows={queryResults.rows} zebra
                           size="compact"></DataTable>
            {:else}
                <pre id="other-results">{JSON.stringify(queryResults, null, 2)}</pre>
            {/if}
        {/if}

    </div>
</main>

<style>
    #main {
        padding: 1rem;
        margin: 0 auto;
        width: 100vw;
        height: 100vh;
        display: flex;
        flex-direction: column;
    }

    :global(#query-area) {
        font-family: monospace;
    }

    :global(.bx--data-table-container) {
        flex: 1;
        overflow: scroll;
    }

    #error-message {
        flex: 1;
        overflow: scroll;
        font-family: monospace;
    }

    #other-results {
        flex: 1;
        overflow: scroll;
        font-family: monospace;
    }
</style>