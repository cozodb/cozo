======================
Getting started
======================

Welcome to the Cozo Manual. The latest version of this manual can be read at https://cozodb.github.io/current/manual.
Alternatively, you can download a PDF version for offline viewing at https://cozodb.github.io/current/manual.pdf.

This manual touches upon all features currently implemented in Cozo,
though the coverage of some topics may be sketchy at this stage.

This manual assumes that you already know the basics of the Cozo database,
at the level of the `Tutorial <https://nbviewer.org/github/cozodb/cozo/blob/main/docs/tutorial/tutorial.ipynb>`_.

------------------------
Downloading Cozo
------------------------

Cozo is distributed as a single executable.
Precompiled binaries can be downloaded from the `release page <https://github.com/cozodb/cozo/releases>`_,
currently available for Linux (Intel x64), Mac (Intel x64 and Apple ARM) and Windows (Intel x64).
For Windows users,
we recommend running Cozo under `WSL <https://learn.microsoft.com/en-us/windows/wsl/install>`_ if possible,
especially if your workload is heavy, as the Windows version runs more slowly.

---------------
Starting Cozo
---------------

Run the ``cozoserver`` command in a terminal::

    ./cozoserver <PATH_TO_DATA_DIRECTORY>

If ``<PATH_TO_DATA_DIRECTORY>`` does not exist, it will be created.
Cozo will then start a web server and bind to address ``127.0.0.1`` and port ``9070``.
These two can be customized: run the executable with the ``-h`` option to learn how.

To stop Cozo, press ``CTRL-C`` in the terminal, or send ``SIGTERM`` to the process with e.g. ``kill``.

-----------------------
The query API
-----------------------

Queries are run by sending HTTP POST requests to the server.
By default, the API endpoint is ``http://127.0.0.1:9070/text-query``.
A JSON body is expected::

    {
        "script": "<COZOSCRIPT QUERY STRING>",
        "params": {}
    }

``params`` should be an object of named parameters.
For example, if ``params`` is ``{"num": 1}``,
then ``$num`` can be used anywhere in your query string where an expression is expected.
Always use ``params`` instead of concatenating strings when you need parametrized queries.

.. WARNING::

    Cozo is designed to run in a trusted environment and be used by trusted clients.
    It does not come with elaborate authentication and security features.
    If you must access Cozo remotely,
    you are responsible for setting up firewalls, encryptions and proxies yourself.

    As a guard against users accidentally exposing sensitive data,
    If you bind Cozo to non-loopback addresses,
    Cozo will generate a token string and require all queries
    from non-loopback addresses to provide the token string
    in the HTTP header field ``x-cozo-auth``.
    The warning printed when you start Cozo with a non-default binding will tell you
    where to find the token string.
    This "security measure" is not considered sufficient for any purpose
    and is only intended as a last defence against carelessness.

--------------------------------------------------
Running queries
--------------------------------------------------

^^^^^^^^^^^^^^^^^^^^^^^^^^
Making HTTP requests
^^^^^^^^^^^^^^^^^^^^^^^^^^

As Cozo has a HTTP-based API,
it is accessible by all languages that are capable of making web requests.
The structure of the API is also deliberately kept minimal so that no dedicated clients are necessary.

As an example, the following runs a system op with the ``curl`` command line tool::

    curl -X POST localhost:9070/text-query \
         -H 'content-type: application/json' \
         -d '{"script": "::running", "params": {}}'

The responses are JSON when queries are successful,
or text descriptions when errors occur,
so a language only needs to be able to process JSON to use Cozo.

^^^^^^^^^^^^^^^^^^^^^^^^^
JupyterLab
^^^^^^^^^^^^^^^^^^^^^^^^^

Cozo has special support for running queries in `JupyterLab <https://jupyterlab.readthedocs.io/en/stable/>`_,
a web-based notebook interface
in the python ecosystem heavily used by data scientists.

First, install JupyterLab by following its instructions.
Then install the ``pycozo`` library::

    pip install "pycozo[pandas]"

Open the JupyterLab web interface, start a Python 3 kernel,
and in a cell run the following `magic command <https://ipython.readthedocs.io/en/stable/interactive/magics.html>`_::

    %load_ext pycozo.ipyext_direct

If you need to connect to Cozo using a non-default address or port,
or you require an authentication string, you need to run the following magic commands as well::

    %cozo_host http://<ADDRESS>:<PORT>
    %cozo_auth <AUTH_STRING>

Now, when you execute cells in the notebook,
the content will be sent to Cozo and interpreted as CozoScript.
Returned relations will be formatted as `Pandas dataframe <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_.

The extension ``pycozo.ipyext_direct`` used above sets up the notebook in the Direct Cozo mode,
where cells are by default interpreted as CozoScript.
Python code can be run by starting the first line of a cell with the ``%%py``.
The Indirect Cozo mode can be started by::

    %load_ext pycozo.ipyext

In this mode, only cells with the first line ``%%cozo`` are interpreted as CozoScript.
Other cells are interpreted in the normal way (python code).
The Indirect mode is useful if you need post-processing and visualizations.

When a query execution is successfully,
the resulting Pandas dataframe will be bound to the python variable ``_``.

A few other magic commands are available:

* ``%cozo_run_file <PATH_TO_FILE>`` runs a local file as CozoScript.
* ``%cozo_run_string <VARIABLE>`` runs variable containing string as CozoScript.
* ``%cozo_set <KEY> <VALUE>`` sets a parameter with the name ``<KEY>`` to the expression ``<VALUE>``.
  The updated parameters will be used by subsequent queries.
* ``%cozo_set_params <PARAM_MAP>`` replace all parameters by the given expression,
  which must evaluate to a dictionary with string keys.
* ``%cozo_clear`` clears all set parameters.
* ``%cozo_params`` returns the parameters currently set.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Python
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can also use the Python client without JupyterLab. First::

    pip install pycozo

Next, in your Python code, do something like the following::

    from pycozo.client import Client

    client = Client(host='http://127.0.0.1:9070', auth=None, dataframe=False)
    print(client.run('::relations'))

If ``dataframe=True``, the client will transform the returned relation into Pandas dataframes, which must be separately installed.
The ``client.run`` method also takes an optional second argument ``params``.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Web Browser
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are reluctant to install python and Jupyter, you may consider the Makeshift JavaScript Console.
To get started, you need a browser on your local machine.
We recommend `Firefox <https://www.mozilla.org/en-US/firefox/new/>`_, `Chrome <https://www.google.com/chrome/>`_,
or any Chromium-based browser for best display.

If Cozo is running under the default configuration,
navigate to ``http://127.0.0.1:9070``.
You should be greeted with a mostly empty page telling you that Cozo is running.
Now open the Developer Console
(`Firefox console <https://firefox-source-docs.mozilla.org/devtools-user/browser_console/index.html>`_
or `Chrome console <https://developer.chrome.com/docs/devtools/console/javascript/>`_)
and switch to the "Console" tab. Now you can execute CozoScript by running::

    await run("<COZOSCRIPT>")

The returned relations will be formatted as tables.
If you need to pass in parameters, provide a second parameter with a JavaScript object::

    await run("<COZOSCRIPT>", <PARAMS>)

If you need to set an auth string, modify the global variable ``COZO_AUTH``.

----------------------------
Building Cozo from source
----------------------------

If for some reason the binary distribution does not work for you,
you can build Cozo from source.
You need to install the `Rust toolchain <https://www.rust-lang.org/tools/install>`_ on your system.
You also need a C++17 compiler.

Clone the Cozo git repo::

    git clone https://github.com/cozodb/cozo.git --recursive

You need to pass the ``--recursive`` flag so that submodules are also cloned. Next, run in the root of the cloned repo::

    cargo build --release

Wait for potentially a long time, and you will find the compiled binary in ``target/release``.

You can run ``cargo build --release -F jemalloc`` instead
to indicate that you want to compile and use jemalloc as the memory allocator for the RocksDB storage backend,
which can make a difference in performance depending on your workload.

--------------------------------
Embedding Cozo
--------------------------------

You can run Cozo in the same process as your main program.

For Rust programs, as ``cozoserver`` is just a very thin wrapper around the Cozo rust library,
you can use the library directly.

For languages other than Rust, you will need to provide custom bindings,
but again for `Python <https://pyo3.rs/>`_ and `NodeJS <https://neon-bindings.com/>`_ this is trivial.