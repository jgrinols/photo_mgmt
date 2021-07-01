"""container module for AggregateResultsError"""

class AggregateResultsError(Exception):
    """an exception to be raised when returning results from an arbitrary
    number of jobs when some may have failed and some may have succeeded"""
    def __init__(self, results, exceptions, *args: object) -> None:
        super().__init__(*args)
        self.results = results
        self.exceptions = exceptions

    def __str__(self):
        return "\n".join([str(e) for e in self.exceptions])
        