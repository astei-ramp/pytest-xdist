import contextlib
import csv

from .loadscope import LoadScopeScheduling
from xdist.remote import Producer


class LoadExternalScheduling(LoadScopeScheduling):
    """Implements load scheduling across nodes based on user-specified scopes,
    defined externally.

    This distributes the tests collected across all nodes, according to a custom
    scope defined by the user.  Each nodeid can only appear in the bucket once.
    All nodes collect and submit the list of tests and when all collections are
    received it is verified they are identical collections.  Tthose work units
    get submitted to nodes.  Whenever a node finishes an item, it calls
    ``.mark_test_complete()`` which will trigger the scheduler
    to assign more work units if the number of pending tests for the node falls
    below a low-watermark.

    When created, ``numnodes`` defines how many nodes are expected to submit a
    collection. This is used to know when all nodes have finished collection.

    This class behaves very much like LoadScopeScheduling, but the scope of each
    test is defined in a side file.
    """

    def __init__(self, config, log=None):
        super().__init__(config, log)

        self.scopes = {}

        if log is None:
            self.log = Producer("loadexternalscope")
        else:
            self.log = log.loadexternalscope

        scope_filename = config.getvalue("scopes")
        if not scope_filename:
            raise RuntimeError("no scopes file specified")

        with open(scope_filename) as scope_file:
            scope_file_reader = csv.reader(scope_file)
            for row in scope_file_reader:
                if len(row) != 2:
                    self.log(
                        "row in durations file doesn't have exactly 2 columns, ignoring"
                    )
                    continue

                nodeid, scope = row
                self.scopes[nodeid] = scope

    def _split_scope(self, nodeid):
        """Determine the scope (grouping) of a nodeid.

        There are usually 3 cases for a nodeid::

            example/loadsuite/test/test_beta.py::test_beta0
            example/loadsuite/test/test_delta.py::Delta1::test_delta0
            example/loadsuite/epsilon/__init__.py::epsilon.epsilon

        #. Function in a test module.
        #. Method of a class in a test module.
        #. Doctest in a function in a package.

        This function will group tests by first looking up by its exact scope,
        then by its class, and then by its file. If no scope is otherwise specified,
        the exact node ID will be used (equivalent to ```loadscope``).

        This function will group tests with the scope determined by splitting
        the first ``::`` from the left. That is, test will be grouped in a
        single work unit when they reside in the same file.
         In the above example, scopes will be::

            example/loadsuite/test/test_beta.py
            example/loadsuite/test/test_delta.py
            example/loadsuite/epsilon/__init__.py
        """
        # look up by the raw nodeid
        scope = self.scopes.get(nodeid)
        if scope:
            return scope

        # strip out any parametrization matrix
        with contextlib.suppress(ValueError):
            parametrized_start_idx = nodeid.index("[")
            nodeid = nodeid[:parametrized_start_idx]

            scope = self.scopes.get(nodeid)
            if scope:
                return scope

        # look at the class (if of form test_a.py::TestClass::test_a) or file (if of form test_a.py::test_a)
        nodeid = "::".join(nodeid.split("::")[:-1])
        scope = self.scopes.get(nodeid)
        if scope:
            return scope

        # look at the file (if of form test_a.py::TestClass::test_a)
        nodeid = "::".join(nodeid.split("::")[:-1])
        if len(nodeid) > 0:
            scope = self.scopes.get(nodeid)
            if scope:
                return scope

        # give up and return the nodeid, to distribute to whichever worker gets it first
        return nodeid
