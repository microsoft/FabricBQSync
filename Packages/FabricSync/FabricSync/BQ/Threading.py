import traceback
from threading import (
    Thread, Lock
)
from typing import (
    Any, List, Tuple
)
from queue import PriorityQueue

from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Model.Core import CommandSet

class ThreadSafeList():
    def __init__(self) -> None:
        """
        Initializes a new instance of the ThreadSafeList class.
        """
        self._list = list()
        self._lock = Lock()

    def append(self, value) -> None:
        """
        Appends the value to the list.
        Args:
            value: The value.
        """
        with self._lock:
            self._list.append(value)

    def pop(self) -> Any:
        """
        Pops the value from the list.
        Returns:
            Any: The value.
        """
        with self._lock:
            return self._list.pop()

    def get(self, index) -> Any:
        """
        Gets the value at the given index.
        Args:
            index: The index.
        Returns:
            Any: The value.
        """
        with self._lock:
            return self._list[index]

    def length(self) -> int:
        """
        Gets the length of the list.
        Returns:
            int: The length of the list.
        """
        with self._lock:
            return len(self._list)

    @property
    def unsafe_list(self) -> list:
        """
        Gets the unsafe list.
        Returns:
            list: The unsafe list.
        """
        with self._lock:
            return self._list

class ThreadSafeDict:
    def __init__(self):
        """
        Initializes a new instance of the ThreadSafeDict class.
        """
        self._dict = {}
        self._lock = Lock()

    def get_or_set(self, key, value) -> Any:
        """
        Gets the value for the given key or sets the value if it does not exist.
        Args:
            key: The key.
            value: The value.
        Returns:
            Any: The value.
        """
        with self._lock:
            if key in self._dict:
                return self._dict.get(key)
            else:
                self._dict[key] = value
                return value

    def set(self, key, value) -> None:
        """
        Sets the value for the given key.
        Args:
            key: The key.
            value: The value.
        """
        with self._lock:
            self._dict[key] = value

    def get(self, key) -> Any:
        """
        Gets the value for the given key.
        Args:
            key: The key.
        Returns:
            Any: The value.
        """
        with self._lock:
            return self._dict.get(key)

    def remove(self, key) -> Any:
        """
        Removes the value for the given key.
        Args:
            key: The key.
        Returns:
            Any: The value.
        """
        with self._lock:
            return self._dict.pop(key, None)

    def contains(self, key) -> bool:
        """
        Determines whether the dictionary contains the given key.
        Args:
            key: The key.
        Returns:
            bool: True if the dictionary contains the key; otherwise, False.
        """
        with self._lock:
            return key in self._dict

    def items(self) -> List:
        """
        Gets the items in the dictionary.
        Returns:
            List: The items in the dictionary.
        """
        with self._lock:
            return list(self._dict.items())

    def keys(self) -> List:
        """
        Gets the keys in the dictionary.
        Returns:
            List: The keys in the dictionary.
        """
        with self._lock:
            return list(self._dict.keys())

    def values(self) -> List:
        """
        Gets the values in the dictionary.
        Returns:
            List: The values in the dictionary.
        """
        with self._lock:
            return list(self._dict.values())
    
    def len(self) -> int:
        """
        Gets the length of the dictionary.
        Returns:
            int: The length of the dictionary.
        """
        with self._lock:
            return len(self._dict)
            
class QueueProcessor(ContextAwareBase):
    def __init__(self, num_threads) -> None:
        """
        Initializes a new instance of the QueueProcessor class.
        Args:
            num_threads: The number of threads.
        """
        self.num_threads = num_threads
        self.workQueue = PriorityQueue()
        self.exceptions = ThreadSafeList()
        self.exception_hook = None

    def process(self, sync_function, exception_hook=None) -> None:
        """
        Processes the queue.
        Args:
            sync_function: The sync function.
            exception_hook: The exception hook.
        """
        self.exception_hook = exception_hook

        for i in range(self.num_threads):
            t=Thread(target=self.__task_runner, args=(sync_function, self.workQueue))
            t.name = f"{SyncConstants.THREAD_PREFIX}_{i}"
            t.daemon = True
            t.start() 
            
        self.workQueue.join()

    def __task_runner(self, sync_function, workQueue:PriorityQueue) -> None:
        """
        Runs the task.
        Args:
            sync_function: The sync function.
            workQueue: The work queue.
        """
        while not workQueue.empty():
            value = workQueue.get()

            try:
                sync_function(value)
            except Exception as e:
                #print(traceback.format_exc())
                self.exceptions.append(e)  
                self.Logger.sync_status(f"QUEUE PROCESS THREAD ERROR: {e}", verbose=True)

                if self.exception_hook:
                    self.exception_hook((*value, e))
            finally:
                workQueue.task_done()

    def put(self, task:callable) -> None:
        """
        Puts the task in the queue.
        Args:
            task: The task.
        """
        self.workQueue.put(item=task)
    
    def empty(self) -> bool:
        """
        Determines whether the queue is empty.
        Returns:
            bool: True if the queue is empty; otherwise, False.
        """
        return self.workQueue.empty()
    
    @property
    def has_exceptions(self) -> bool:
        """
        Determines whether the queue has exceptions.
        Returns:
            bool: True if the queue has exceptions; otherwise, False.
        """
        return self.exceptions.length() > 0

class SparkProcessor(ContextAwareBase):
    @classmethod
    def _processor(cls, cmd, num_threads=10) -> None:
        """
        Processes the command.
        Args:
            cmd: The command.
            num_threads: The number of threads.
        """
        processor = QueueProcessor(num_threads)

        for c in cmd:
            if isinstance(c, CommandSet):
                processor.put((c.Priority, c))
            else:
                processor.put((1, CommandSet(c)))
        
        processor.process(cls._process_command)

    @classmethod
    def process_command_list(cls, commands:List[str]) -> None:
        """
        Processes the command list.
        Args:
            commands: The commands.
        """ 
        commands = [CommandSet(c) for c in commands]
        cls._processor(commands)

    @classmethod
    def delete_from(cls, tables:List[str], schema:str=None) -> None:
        """
        Deletes the data from the given tables.
        Args:
            tables: The tables.
            schema: The schema.
        """
        if not schema:
            commands = [CommandSet(f"DELETE FROM {t};") for t in tables]
        else:
            commands = [CommandSet(f"DELETE FROM {schema}.{t};") for t in tables]

        cls._processor(commands)

    @classmethod
    def drop(cls, tables:List[str], schema:str=None) -> None:
        """
        Drops the given tables.
        Args:
            tables: The tables.
            schema: The schema.
        """
        if not schema:
            commands = [CommandSet(f"DROP TABLE IF EXISTS {t};") for t in tables]
        else:
            commands = [CommandSet(f"DROP TABLE IF EXISTS {schema}.{t};") for t in tables]

        cls._processor(commands)

    @classmethod
    def optimize_vacuum(cls, tables:List[str], schema:str=None) -> None:
        """
        Optimizes and vacuums the given tables.
        Args:
            tables: The tables.
            schema: The schema.
        """
        if not schema:
            commands=[CommandSet([f"OPTIMIZE {t};",f"VACUUM {t};"]) for t in tables]
        else:
            commands=[CommandSet([f"OPTIMIZE {schema}.{t};",f"VACUUM {schema}.{t};"]) for t in tables]

        cls._processor(commands)

    @classmethod
    def _process_command(cls, value:Tuple) -> None:
        """
        Processes the command.
        Args:
            value: The value.
        """
        cmd = value[1]

        for c in cmd.Commands:
            cls.Context.sql(c)