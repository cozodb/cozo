import {KernelMessage} from '@jupyterlab/services';

import {BaseKernel} from '@jupyterlite/kernel';

/**
 * A kernel that echos content back.
 */
export class CozoScriptKernel extends BaseKernel {
    /**
     * Handle a kernel_info_request message
     */
    async kernelInfoRequest(): Promise<KernelMessage.IInfoReplyMsg['content']> {
        const content: KernelMessage.IInfoReply = {
            implementation: 'Text',
            implementation_version: '0.1.0',
            language_info: {
                codemirror_mode: {
                    name: 'text/plain'
                },
                file_extension: '.txt',
                mimetype: 'text/plain',
                name: 'cozo',
                nbconvert_exporter: 'text',
                pygments_lexer: 'text',
                version: 'es2017'
            },
            protocol_version: '5.3',
            status: 'ok',
            banner: 'CozoScript playground',
            help_links: []
        };
        return content;
    }

    /**
     * Handle an `execute_request` message
     *
     * @param msg The parent message.
     */
    async executeRequest(
        content: KernelMessage.IExecuteRequestMsg['content']
    ): Promise<KernelMessage.IExecuteReplyMsg['content']> {
        // const url = 'http://127.0.0.1:9070/text-query';
        const url = '/text-query';

        const {code} = content;

        try {
            let response = await fetch(url, {
                method: 'POST',
                body: JSON.stringify({script: code, params: {}}),
                headers: new Headers({
                    'content-type': 'application/json',
                }),
            });
            if (!response.ok) {
                this.publishExecuteResult({
                    execution_count: this.executionCount,
                    data: {
                        'text/html': `<pre style="font-size: small">${await response.text()}</pre>`
                    },
                    metadata: {}
                });
            } else {
                // let res = await response.json();
                this.publishExecuteResult({
                    execution_count: this.executionCount,
                    data: {
                        'text/html': displayTable(await response.json())
                    },
                    metadata: {}
                });
            }
        } catch (e) {
            console.error(e);
        }

        return {
            status: 'ok',
            execution_count: this.executionCount,
            user_expressions: {}
        };
    }

    /**
     * Handle an complete_request message
     *
     * @param msg The parent message.
     */
    async completeRequest(
        content: KernelMessage.ICompleteRequestMsg['content']
    ): Promise<KernelMessage.ICompleteReplyMsg['content']> {
        throw new Error('Not implemented');
    }

    /**
     * Handle an `inspect_request` message.
     *
     * @param content - The content of the request.
     *
     * @returns A promise that resolves with the response message.
     */
    async inspectRequest(
        content: KernelMessage.IInspectRequestMsg['content']
    ): Promise<KernelMessage.IInspectReplyMsg['content']> {
        throw new Error('Not implemented');
    }

    /**
     * Handle an `is_complete_request` message.
     *
     * @param content - The content of the request.
     *
     * @returns A promise that resolves with the response message.
     */
    async isCompleteRequest(
        content: KernelMessage.IIsCompleteRequestMsg['content']
    ): Promise<KernelMessage.IIsCompleteReplyMsg['content']> {
        throw new Error('Not implemented');
    }

    /**
     * Handle a `comm_info_request` message.
     *
     * @param content - The content of the request.
     *
     * @returns A promise that resolves with the response message.
     */
    async commInfoRequest(
        content: KernelMessage.ICommInfoRequestMsg['content']
    ): Promise<KernelMessage.ICommInfoReplyMsg['content']> {
        throw new Error('Not implemented');
    }

    /**
     * Send an `input_reply` message.
     *
     * @param content - The content of the reply.
     */
    inputReply(content: KernelMessage.IInputReplyMsg['content']): void {
        throw new Error('Not implemented');
    }

    /**
     * Send an `comm_open` message.
     *
     * @param msg - The comm_open message.
     */
    async commOpen(msg: KernelMessage.ICommOpenMsg): Promise<void> {
        throw new Error('Not implemented');
    }

    /**
     * Send an `comm_msg` message.
     *
     * @param msg - The comm_msg message.
     */
    async commMsg(msg: KernelMessage.ICommMsgMsg): Promise<void> {
        throw new Error('Not implemented');
    }

    /**
     * Send an `comm_close` message.
     *
     * @param close - The comm_close message.
     */
    async commClose(msg: KernelMessage.ICommCloseMsg): Promise<void> {
        throw new Error('Not implemented');
    }
}

function escapeHtml(unsafe: string) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function displayValue(v: any) {
    if (typeof v === 'number') {
        return `<span style="color: #307fc1;">${v}</span>`
    } else if (typeof v === "string") {
        return escapeHtml(v)
    } else {
        return `<span style="color: #bf5b3d;">${escapeHtml(JSON.stringify(v))}</span>`
    }
}

function displayTable(v: any) {
    let ret = '<div style="display: flex; align-items: end; flex-direction: row;"><table>';

    if (v.headers) {
        ret += '<thead><tr>'
        for (const head of v.headers) {
            ret += '<td>';
            ret += displayValue(head);
            ret += '</td>';
        }
        ret += '</tr></thead>'
    }

    ret += '<tbody>'
    for (const row of v.rows) {
        ret += '<tr>'
        for (const el of row) {
            ret += '<td>';
            ret += displayValue(el);
            ret += '</td>';
        }
        ret += '</tr>'
    }
    ret += '</tbody></table>';
    if (typeof v.time_taken === 'number') {
        ret += `<span style="color: darkgrey; font-size: xx-small; margin: 13px;">Took ${v.time_taken}ms</span>`
    }
    ret += '</div>'
    return ret;
}

