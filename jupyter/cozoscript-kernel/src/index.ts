// Copyright (c) JupyterLite Contributors
// Distributed under the terms of the Modified BSD License.

import {JupyterLiteServer, JupyterLiteServerPlugin} from '@jupyterlite/server';

import {IKernel, IKernelSpecs} from '@jupyterlite/kernel';

import {CozoScriptKernel} from './kernel';

/**
 * A plugin to register the echo kernel.
 */
const kernel: JupyterLiteServerPlugin<void> = {
    id: 'cozoscript-notebook:kernel',
    autoStart: true,
    requires: [IKernelSpecs],
    activate: (app: JupyterLiteServer, kernelspecs: IKernelSpecs) => {
        kernelspecs.register({
            spec: {
                name: 'cozo',
                display_name: 'CozoScript (localhost)',
                language: 'text',
                argv: [],
                resources: {
                    'logo-32x32': '',
                    'logo-64x64': ''
                }
            },
            create: async (options: IKernel.IOptions): Promise<IKernel> => {
                return new CozoScriptKernel(options);
            }
        });

        console.log('the CozoScript kernel is activated NOW!');
    }
};

const plugins: JupyterLiteServerPlugin<any>[] = [kernel];

export default plugins;
