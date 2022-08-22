<script>
    import { Button } from "carbon-components-svelte";
    import {message, open, save} from '@tauri-apps/api/dialog';
    import {invoke} from '@tauri-apps/api/tauri'
    import {Folder, FolderAdd, Close, DirectionStraightRight} from "carbon-icons-svelte";

    let queryText = '';
    let queryArea;

    let hasError = false;
    let resultText = '';
    let started;
    let inProgress = false;
    let statusMessage = '';

    async function open_db(path) {
        if (path) {
            try {
                await invoke('open_db', {path})
            } catch (e) {
                await message('' + e, {type: 'error', title: 'Cannot open'})
            }
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

    async function handleClose() {
        try {
            await invoke('close_db');
        } catch (e) {
            await message('' + e, {type: 'error', title: 'Cannot close'});
        }
    }

    async function handleQuery() {
        const query = queryText.trim();
        if (query) {
            hasError = false;
            inProgress = true;
            started = performance.now();
            statusMessage = 'Querying ...'
            try {
                let res = await invoke('run_query', {query});
                // await message(JSON.stringify(res, null, 2), 'Result')
                resultText = JSON.stringify(res, null, 2);
            } catch (e) {
                resultText = '' + e;
                hasError = true;
                // await message('' + e, {type: 'error', title: 'Cannot query'})
            }
            finally {
                inProgress = false;
                let time = Math.round(performance.now() - started);
                statusMessage = `Query finished in ${time}ms`
            }
        }
        queryArea.focus();
    }

</script>

<main>
    <div>
        <Button icon={Folder} on:click={handleOpen}>Open</Button>
        <Button icon={FolderAdd} on:click={handleCreate}>Create</Button>
        <Button icon={Close} on:click={handleClose}>Close</Button>
    </div>
    <textarea bind:this={queryArea} bind:value={queryText}></textarea>
    <div>
        <Button icon={DirectionStraightRight} on:click={handleQuery} disabled={inProgress}>Query</Button>
    </div>
    <div>{statusMessage}</div>
    <div class="result-display">
        <pre class:hasError>
            {resultText}
        </pre>
    </div>
</main>

<style>
    main {
        padding: 1em;
        margin: 0 auto;
    }

    textarea {
        width: 100%;
        height: 200px;
        font-family: monospace;
    }

    .hasError {
        color: #ff3e00;
    }
    .result-display {
        text-align: left;
    }
</style>