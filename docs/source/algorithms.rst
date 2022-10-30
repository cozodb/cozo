==============================
Utilities and algorithms
==============================

Fixed rules in CozoScript apply utilities or algorithms.

.. module:: Algo
    :noindex:


-------------------
Utilities
-------------------

.. function:: Constant(data: [...])

    Returns a relation containing the data passed in. The constant rule ``?[] <- ...`` is
    syntax sugar for ``?[] <~ Constant(data: ...)``.

    :param data: A list of lists, representing the rows of the returned relation.

.. function:: ReorderSort(rel[...], out: [...], sort_by: [...], descending: false, break_ties: false, skip: 0, take: 0)

    Sort and then extract new columns of the passed in relation ``rel``.

    :param required out: A list of expressions which will be used to produce the output relation. Any bindings in the expressions will be bound to the named positions in ``rel``.
    :param sort_by: A list of expressions which will be used to produce the sort keys. Any bindings in the expressions  will be bound to the named positions in ``rel``.
    :param descending: Whether the sorting process should be done in descending order. Defaults to ``false``.
    :param break_ties: Whether ties should be broken, e.g. whether the first two rows with *identical sort keys* should be given ordering numbers ``1`` and ``2`` instead of ``1`` and ``1``. Defaults to false.
    :param skip: How many rows to skip before producing rows. Defaults to zero.
    :param take: How many rows at most to produce. Zero means no limit. Defaults to zero.
    :return: The returned relation, in addition to the rows specified in the parameter ``out``, will have the ordering prepended. The ordering starts at ``1``.

    .. TIP::

        This algorithm serves a similar purpose to the global ``:order``, ``:limit`` and ``:offset`` options, but can be applied to intermediate results. Prefer the global options if it is applied to the final output.

.. function:: CsvReader(url: ..., types: [...], delimiter: ',', prepend_index: false, has_headers: true)

    Read a CSV file from disk or an HTTP GET request and convert the result to a relation.

    :param required url: URL for the CSV file. For local file, use ``file://<PATH_TO_FILE>``.
    :param required types: A list of strings interpreted as types for the columns of the output relation. If any type is specified as nullable and conversion to the specified type fails, ``null`` will be the result. This is more lenient than other functions since CSVs tend to contain lots of bad values.
    :param delimiter: The delimiter to use when parsing the CSV file.
    :param prepend_index: If ``true``, row index will be prepended to the columns.
    :param has_headers: Whether the CSV file has headers. The reader will not interpret the header in any way but will instead simply ignore it.

.. function:: JsonReader(url: ..., fields: [...], json_lines: true, null_if_absent: false, prepend_index: false)

    Read a JSON file for disk or an HTTP GET request and convert the result to a relation.

    :param required url: URL for the JSON file. For local file, use ``file://<PATH_TO_FILE>``.
    :param required fields: A list of field names, for extracting fields from JSON arrays into the relation.
    :param json_lines: If ``true``, parse the file as lines of JSON objects, each line containing a single object; if false, parse the file as a JSON array containing many objects.
    :param null_if_absent: If a ``true`` and a requested field is absent, will output ``null`` in its place. If ``false`` and the requested field is absent, will throw an error.
    :param prepend_index: If ``true``, row index will be prepended to the columns.

------------------------------------
Connectedness algorithms
------------------------------------

.. function:: ConnectedComponents(edges[from, to])

    Computes the `connected components <https://en.wikipedia.org/wiki/Connected_component_(graph_theory)>`_ of a graph with the provided edges.

    :return: Pairs containing the node index, and its component index.


.. function:: StronglyConnectedComponent(edges[from, to])

    Computes the `strongly connected components <https://en.wikipedia.org/wiki/Strongly_connected_component>`_ of a graph with the provided edges.

    :return: Pairs containing the node index, and its component index.

.. function:: SCC(...)

    See :func:`Algo.StronglyConnectedComponent`.

.. function:: MinimumSpanningForestKruskal(edges[from, to, weight?])

    Runs `Kruskal's algorithm <https://en.wikipedia.org/wiki/Kruskal%27s_algorithm>`_ on the provided edges to compute a `minimum spanning forest <https://en.wikipedia.org/wiki/Minimum_spanning_tree>`_. Negative weights are fine.

    :return: Triples containing the from-node, the to-node, and the cost from the tree root to the to-node. Which nodes are chosen to be the roots are non-deterministic. Multiple roots imply the graph is disconnected.

.. function:: MinimumSpanningTreePrim(edges[from, to, weight?], starting?[idx])

    Runs `Prim's algorithm <https://en.wikipedia.org/wiki/Prim%27s_algorithm>`_ on the provided edges to compute a `minimum spanning tree <https://en.wikipedia.org/wiki/Minimum_spanning_tree>`_. ``starting`` should be a relation producing exactly one node index as the starting node. Only the connected component of the starting node is returned. If ``starting`` is omitted, which component is returned is arbitrary.

    :return: Triples containing the from-node, the to-node, and the cost from the tree root to the to-node.

.. function:: TopSort(edges[from, to])

    Performs `topological sorting <https://en.wikipedia.org/wiki/Topological_sorting>`_ on the graph with the provided edges. The graph is required to be connected in the first place.

    :return: Pairs containing the sort order and the node index.

------------------------------------
Pathfinding algorithms
------------------------------------

.. function:: ShortestPathDijkstra(edges[from, to, weight?], starting[idx], goals[idx], undirected: false, keep_ties: false)

    Runs `Dijkstra's algorithm <https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm>`_ to determine the shortest paths between the ``starting`` nodes and the ``goals``. Weights, if given, must be non-negative.

    :param undirected: Whether the graph should be interpreted as undirected. Defaults to ``false``.
    :param keep_ties: Whether to return all paths with the same lowest cost. Defaults to ``false``, in which any one path of the lowest cost could be returned.
    :return: 4-tuples containing the starting node, the goal, the lowest cost, and a path with the lowest cost.

.. function:: KShortestPathYen(edges[from, to, weight?], starting[idx], goals[idx], k: expr, undirected: false)

    Runs `Yen's algorithm <https://en.wikipedia.org/wiki/Yen%27s_algorithm>`_ (backed by Dijkstra's algorithm) to find the k-shortest paths between nodes in ``starting`` and nodes in ``goals``.

    :param required k: How many routes to return for each start-goal pair.
    :param undirected: Whether the graph should be interpreted as undirected. Defaults to ``false``.
    :return: 4-tuples containing the starting node, the goal, the cost, and a path with the cost.

.. function:: BreadthFirstSearch(edges[from, to], nodes[idx, ...], starting?[idx], condition: expr, limit: 1)

    Runs breadth first search on the directed graph with the given edges and nodes, starting at the nodes in ``starting``. If ``starting`` is not given, it will default to all of ``nodes``, which may be quite a lot to calculate.

    :param required condition: The stopping condition, will be evaluated with the bindings given to ``nodes``. Should evaluate to a boolean, with ``true`` indicating an acceptable answer was found.
    :param limit: How many answers to produce for each starting nodes. Defaults to 1.
    :return: Triples containing the starting node, the answer node, and the found path connecting them.

.. function:: BFS(...)

    See :func:`Algo.BreadthFirstSearch`.


.. function:: DepthFirstSearch(edges[from, to], nodes[idx, ...], starting?[idx], condition: expr, limit: 1)

    Runs depth first search on the directed graph with the given edges and nodes, starting at the nodes in ``starting``. If ``starting`` is not given, it will default to all of ``nodes``, which may be quite a lot to calculate.

    :param required condition: The stopping condition, will be evaluated with the bindings given to ``nodes``. Should evaluate to a boolean, with ``true`` indicating an acceptable answer was found.
    :param limit: How many answers to produce for each starting nodes. Defaults to 1.
    :return: Triples containing the starting node, the answer node, and the found path connecting them.

.. function:: DFS(...)

    See :func:`Algo.DepthFirstSearch`.

.. function:: ShortestPathAStar(edges[from, to, weight], nodes[idx, ...], starting[idx], goals[idx], heuristic: expr)

    Computes the shortest path from every node in ``starting`` to every node in ``goals`` by the `A\* algorithm <https://en.wikipedia.org/wiki/A*_search_algorithm>`_.

    ``edges`` are interpreted as directed, weighted edges with non-negative weights.

    :param required heuristic: The search heuristic expression. It will be evaluated with the bindings from ``goals`` and ``nodes``. It should return a number which is a lower bound of the true shortest distance from a node to the goal node. If the estimate is not a valid lower-bound, i.e. it over-estimates, the results returned may not be correct.

    :return: 4-tuples containing the starting node index, the goal node index, the lowest cost, and a path with the lowest cost.

    .. TIP::

        The performance of A\* star algorithm heavily depends on how good your heuristic function is. Passing in ``0`` as the estimate is always valid, but then you really should be using Dijkstra's algorithm.

        Good heuristics usually come about from a metric in the ambient space in which your data live, e.g. spherical distance on the surface of a sphere, or Manhattan distance on a grid. :func:`Func.Math.haversine_deg_input` could be helpful for the spherical case. Note that you must use the correct units for the distance.

        Providing a heuristic that is not guaranteed to be a lower-bound *might* be acceptable if you are fine with inaccuracies. The errors in the answers are bound by the sum of the margins of your over-estimates.

-------------------------------------
Community detection algorithms
-------------------------------------

.. function:: ClusteringCoefficients(edges[from, to, weight?])

    Computes the `clustering coefficients <https://en.wikipedia.org/wiki/Clustering_coefficient>`_ of the graph with the provided edges.

    :return: 4-tuples containing the node index, the clustering coefficient, the number of triangles attached to the node, and the total degree of the node.

.. function:: CommunityDetectionLouvain(edges[from, to, weight?], undirected: false, max_iter: 10, delta: 0.0001, keep_depth?: depth)

    Runs the `Louvain algorithm <https://en.wikipedia.org/wiki/Louvain_method>`_ on the graph with the provided edges, optionally non-negatively weighted.

    :param undirected: Whether the graph should be interpreted as undirected. Defaults to ``false``.
    :param max_iter: The maximum number of iterations to run within each epoch of the algorithm. Defaults to 10.
    :param delta: How much the `modularity <https://en.wikipedia.org/wiki/Modularity_(networks)>`_ has to change before a step in the algorithm is considered to be an improvement.
    :param keep_depth: How many levels in the hierarchy of communities to keep in the final result. If omitted, all levels are kept.
    :return: Pairs containing the label for a community, and a node index belonging to the community. Each label is a list of integers with maximum length constrained by the parameter ``keep_depth``.  This list represents the hierarchy of sub-communities containing the list.

.. function:: LabelPropagation(edges[from, to, weight?], undirected: false, max_iter: 10)

    Runs the `label propagation algorithm <https://en.wikipedia.org/wiki/Label_propagation_algorithm>`_ on the graph with the provided edges, optionally weighted.

    :param undirected: Whether the graph should be interpreted as undirected. Defaults to ``false``.
    :param max_iter: The maximum number of iterations to run. Defaults to 10.
    :return: Pairs containing the integer label for a community, and a node index belonging to the community.

-------------------------------------
Centrality measures
-------------------------------------

.. function:: DegreeCentrality(edges[from, to])

    Computes the degree centrality of the nodes in the graph with the given edges. The computation is trivial, so this should be your first thing to try when exploring new data.

    :return: 4-tuples containing the node index, the total degree (how many edges involve this node), the out-degree (how many edges point away from this node), and the in-degree (how many edges point to this node).

.. function:: PageRank(edges[from, to, weight?], undirected: false, theta: 0.8, epsilon: 0.05, iterations: 20)

    Computes the `PageRank <https://en.wikipedia.org/wiki/PageRank>`_ from the given graph with the provided edges, optionally weighted.

    :param undirected: Whether the graph should be interpreted as undirected. Defaults to ``false``.
    :param theta: A number between 0 and 1 indicating how much weight in the PageRank matrix is due to the explicit edges. A number of 1 indicates no random restarts. Defaults to 0.8.
    :param epsilon: Minimum PageRank change in any node for an iteration to be considered an improvement. Defaults to 0.05.
    :param iterations: How many iterations to run. Fewer iterations are run if convergence is reached. Defaults to 20.

    :return: Pairs containing the node label and its PageRank. For a graph with uniform edges, the PageRank of every node is 1. The `L2-norm <https://en.wikipedia.org/wiki/Norm_(mathematics)>`_ of the results is forced to be invariant, i.e. in the results those nodes with a PageRank greater than 1 is "more central" than the average node in a certain sense.

.. function:: ClosenessCentrality(edges[from, to, weight?], undirected: false)

    Computes the `closeness centrality <https://en.wikipedia.org/wiki/Closeness_centrality>`_ of the graph. The input relation represent edges connecting node indices which are optionally weighted.

    :param undirected: Whether the edges should be interpreted as undirected. Defaults to ``false``.
    :return: Node index together with its centrality.

.. function:: BetweennessCentrality(edges[from, to, weight?], undirected: false)

    Computes the `betweenness centrality <https://en.wikipedia.org/wiki/Betweenness_centrality>`_ of the graph. The input relation represent edges connecting node indices which are optionally weighted.

    :param undirected: Whether the edges should be interpreted as undirected. Defaults to ``false``.
    :return: Node index together with its centrality.

    .. WARNING::

        ``BetweennessCentrality`` is very expensive for medium to large graphs. If possible, collapse large graphs into supergraphs by running a community detection algorithm first.

------------------
Miscellaneous
------------------

.. function:: RandomWalk(edges[from, to, ...], nodes[idx, ...], starting[idx], steps: 10, weight?: expr, iterations: 1)

    Performs random walk on the graph with the provided edges and nodes, starting at the nodes in ``starting``.

    :param required steps: How many steps to walk for each node in ``starting``. Produced paths may be shorter if dead ends are reached.
    :param weight: An expression evaluated against bindings of ``nodes`` and bindings of ``edges``, at a time when the walk is at a node and choosing between multiple edges to follow. It should evaluate to a non-negative number indicating the weight of the given choice of edge to follow. If omitted, which edge to follow is chosen uniformly.
    :param iterations: How many times walking is repeated for each starting node.
    :return: Triples containing a numerical index for the walk, the starting node, and the path followed.
