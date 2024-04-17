class ScheduleDAG:
    def __init__(self, timeout=7200, concurrency=5):
        self.activities = []
        self.timeoutInSeconds = timeout
        self.concurrency = concurrency

class DAGActivity:
    def __init__(self, name, path, timeout = 3600, retry =  None, retryInterval = None, dependencies = [], **keyword_args):
        self.name = name
        self.path = path
        self.timeoutPerCellInSeconds = timeout
        self.retry = retry
        self.retryIntervalInSeconds = retryInterval
        self.dependencies = dependencies
        self.args = keyword_args

class ScheduleDAGEncoder(JSONEncoder):
        def default(self, o):
            return o.__dict__