======================
Getting started
======================

Cozo is distributed as a single executable.
To get started, download the executable for your platform and uncompress it.
After decompression, you may also need to give it executable permission by ``chmod +x ./cozo`` on Unix-based systems.

The pre-compiled distributions of Cozo support Linux, Mac and Windows.
As the toolchain on Windows is very different from UNIX-based systems,
the Windows build hasn't received as much attention as the other builds,
and may suffer from inferior performance and Windows-specific bugs.
For Windows users,
we recommend running Cozo under `WSL <https://learn.microsoft.com/en-us/windows/wsl/install>`_ if possible,
especially if your workload is heavy.

---------------
Running Cozo
---------------

Run the ``cozo`` command in a terminal::

    ./cozo PATH_TO_DATA_DIRECTORY

If ``PATH_TO_DATA_DIRECTORY`` does not exist, it will be created. Cozo will then start a web server and bind to address ``127.0.0.1`` and port ``9070``. These two can be customized: run the executable with the ``-h`` option to learn how.

To stop Cozo, type ``CTRL-C`` in the terminal, or send ``SIGTERM`` to the process with e.g. ``kill``.

-----------------------
The query API
-----------------------

Queries are run by sending HTTP POST requests to the server. By default, the API endpoint is ``http://127.0.0.1:9070/text-query``. The structure of the expected JSON payload is::

    {
        "script": "<COZOSCRIPT QUERY STRING>",
        "params": {}
    }

``params`` should be an object of named parameters. For example, if you have ``params`` set up to be ``{"num": 1}``, then ``$num`` can be used anywhere in your query string where an expression is expected. Always use ``params`` instead of constructing query strings yourself when you have parametrized queries.

---------------
Security
---------------

Cozo is currently designed to run in a trusted environment and be used by trusted clients, therefore it does not come with elaborate authentication and security features. If you must access Cozo remotely, you are responsible for setting up firewalls, encryptions and proxies yourself.

As a guard against users carelessly binding Cozo to any address other than ``127.0.0.1`` and potentially exposing content to everyone on the Internet, in this case, Cozo will refuse to start unless you also set up the environment variable ``COZO_AUTH``. With the variable set, Cozo will then require all queries to provide the content of the set variable in the HTTP header field ``x-cozo-auth`` for verification. Please note that this "security measure" is not considered sufficient for any purpose and is only a last defence when every other security measure that you are responsible for setting up fails.

--------------------------------------------------
Ways of running queries
--------------------------------------------------

^^^^^^^^^^^^^^^^^^^^^^^^^^
Making HTTP requests
^^^^^^^^^^^^^^^^^^^^^^^^^^

As Cozo has a web-based API, it is accessible by all languages that are capable of making web requests. The structure of the API is also deliberately kept minimal so that no dedicated clients are necessary. The return values of requests are JSON when requests are successful, or text descriptions when errors occur, so a language only needs to be able to process JSON to use Cozo.


^^^^^^^^^^^^^^^^^^^^^^^^^
JupyterLab
^^^^^^^^^^^^^^^^^^^^^^^^^

`JupyterLab <https://jupyterlab.readthedocs.io/en/stable/>`_ is a web-based notebook interface in the python ecosystem heavily used by data scientists and is the recommended "IDE" of Cozo.

First, install JupyterLab by following the install instructions of the project. Then install the pycozo library by running::

    pip install "pycozo[pandas]"

Now, open the JupyterLab web interface, start a Python 3 kernel, and in a cell run the following `magic command <https://ipython.readthedocs.io/en/stable/interactive/magics.html>`_::

    %load_ext pycozo.ipyext_direct

If you need to connect to Cozo using a non-default address or port, or you require an authentication string, you need to run the following magic commands as well::

    %cozo_host http://<BIND_ADDRESS>:<PORT>
    %cozo_auth <YOUR_AUTH_STRING>

Now you can execute cells as you usually do in JupyterLab, and the content of the cells will be sent to Cozo and interpreted as CozoScript. Returned relations will be formatted as `Pandas dataframe <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_.

The above sets up the notebook in the Direct Cozo mode, where cells are default interpreted as CozoScript. You can still execute python code by starting the first line of a cell with the ``%%py``. There is also an Indirect Cozo mode, started by::

    %load_ext pycozo.ipyext

In this mode, only cells with the first line content ``%%cozo`` are interpreted as CozoScript. Other cells are interpreted in the normal way (by default, python code). Which mode you use depends on your workflow. We recommend the Indirect mode if you have lots of post-processing and visualizations.

When a query is successfully executed, the result will be bound to the python variable ``_`` as a Pandas dataframe (this is a feature of Jupyter notebooks: the Cozo extension didn't do anything extra).

There are a few other useful magic commands:

* ``%cozo_run_file <PATH_TO_FILE>`` runs a local file as CozoScript.
* ``%cozo_run_string <VARIABLE>`` runs variable containing string as CozoScript.
* ``%cozo_set <KEY> <VALUE>`` sets a parameter with the name ``<KEY>`` to the expression ``<VALUE>``. The set parameters will be used by subsequent queries.
* ``%cozo_set_params <PARAM_MAP>`` replace all parameters by the given expression, which must evaluate to a dictionary with string keys.
* ``%cozo_clear`` clears all set parameters.
* ``%cozo_params`` returns the parameters currently set.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The Makeshift JavaScript Console
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Python and JupyterLab ecosystem is rather heavy-weight. If you are just testing out or running Cozo in an environment that only occasionally requires manual queries, you may be reluctant to install them. In this case, you may find the Makeshift JavaScript Console helpful.

As Cozo is running an HTTP service, we assume that the browser on your local machine can reach its network. We recommend `Firefox <https://www.mozilla.org/en-US/firefox/new/>`_, `Chrome <https://www.google.com/chrome/>`_, or any Chromium-based browser for best display.

If Cozo is running under the default configuration, navigate to ``http://127.0.0.1:9070``. You should be greeted with a mostly empty page telling you that Cozo is running. Now open the Developer Console (`Firefox console <https://firefox-source-docs.mozilla.org/devtools-user/browser_console/index.html>`_ or `Chrome console <https://developer.chrome.com/docs/devtools/console/javascript/>`_) and switch to the "Console" tab. Now you can execute CozoScript by running::

    await run("<COZOSCRIPT>")

The returned tables will be properly formatted. If you need to pass in parameters, provide a second parameter with a JavaScript object. If you need to set an auth string, modify the global variable ``COZO_AUTH``.

The JavaScript Console is not as nice to use as Jupyter notebooks, but we think that it provides a much better experience than hand-rolled CLI consoles, since you can use JavaScript to manipulate the results.