<script>
    let queryText = '';

    let queryResults = null;
    let started;
    let inProgress = false;
    let errorMessage = '';
    let statusMessage = '';

    import {invoke} from "@tauri-apps/api/tauri";
    import {message, open, save} from "@tauri-apps/api/dialog"
    import {Button, DataTable, InlineLoading, Link, TextArea} from "carbon-components-svelte";
    import {DirectionStraightRight, Folder, FolderAdd} from "carbon-icons-svelte";
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
                const res = await invoke('run_query', {query});
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
            } catch (e) {
                errorMessage = '' + e;
            } finally {
                inProgress = false;
                let time = Math.round(performance.now() - started);
                statusMessage = `finished in ${time}ms`
            }
        }
        document.getElementById("query-area").focus();
    }

    let recent = [];

    onMount(async () => {
        const recentData = localStorage.getItem("recent_dbs");
        if (recentData) {
            try {
                recent = JSON.parse(recentData);
                if (recent.length) {
                    let is_opened = await invoke('is_opened');
                    if (is_opened) {
                        db_opened = recent[0];
                    }
                }
            } catch (e) {
            }
        }
    })

    async function open_db(path) {
        if (path) {
            try {
                await invoke('open_db', {path});
                onOpenedDb(path);
            } catch (e) {
                await message('' + e, {type: 'error', title: 'Cannot open'})
            }
        }

    }

    function onOpenedDb(new_db_path) {
        db_opened = new_db_path;
        if (!recent.includes(new_db_path)) {
            recent.unshift(new_db_path);
            recent = recent.slice(0, 10);
            localStorage.setItem("recent_dbs", JSON.stringify(recent))
        }
    }

    async function handleOpen() {
        const path = await open({directory: true});
        await open_db(path);
    }

    async function handleCreate() {
        const path = await save();
        await open_db(path);
    }

    async function handleOpenRecent(db) {
        await open_db(db)
    }

    let db_opened = null;
</script>

<main>
    <div id="main">
        {#if db_opened}
            <div id="upper">
                <TextArea bind:value={queryText} labelText={`Using database ${db_opened}`} rows={10}
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
                <DataTable headers={queryResults.headers} rows={queryResults.rows} zebra
                           size="compact"></DataTable>
            {/if}
        {:else}
            <div style="padding: 0.5em 0.5em 1em">
                <h1 style="padding-bottom: 0.5em">Start</h1>
                <Button size="xl" icon={Folder} on:click={handleOpen}>Open existing</Button>
                <Button size="xl" icon={FolderAdd} on:click={handleCreate}>Create new</Button>

                {#if recent.length}
                    <div style="padding-top: 1em; padding-left: 0.5em">
                        {#each recent as dbPath}
                            <Link style="cursor: pointer" on:click={() => handleOpenRecent(dbPath)}>{dbPath}</Link>
                        {/each}
                    </div>
                {/if}

            </div>
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
        font-family: monospace;
    }
</style>