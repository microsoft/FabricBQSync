import traceback
import threading
from threading import (
    Thread, Lock, Event
)
from typing import (
    Any, List, Tuple
)
from queue import PriorityQueue

from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Model.Core import CommandSet

class ThreadSafeList():
    """
    A thread-safe list that allows multiple threads to append, pop, and get values safely.
    This class uses a lock to ensure that only one thread can modify the list at a time.
    Attributes:
        _list (list): The underlying list.
        _lock (Lock): A lock to ensure thread safety.
    Methods:
        __init__() -> None:
            Initializes a new instance of the ThreadSafeList class.
        append(value) -> None:
            Appends the value to the list.
        pop() -> Any:
            Pops the value from the list.
        get(index) -> Any:
            Gets the value at the given index.
        length() -> int:
            Gets the length of the list.
        unsafe_list() -> list:
            Gets the unsafe list.
    """
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

    def clear(self) -> None:
        """
        Clears the list - This method empties the list and releases any resources associated with it.
        Returns:
            None
        """
        with self._lock:
            return self._list.clear()
        
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
    """
    A thread-safe dictionary that allows multiple threads to get, set, and remove values safely.
    This class uses a lock to ensure that only one thread can modify the dictionary at a time.
    Attributes:
        _dict (dict): The underlying dictionary.
        _lock (Lock): A lock to ensure thread safety.
    Methods:
        __init__() -> None:
            Initializes a new instance of the ThreadSafeDict class.
        get_or_set(key, value) -> Any:
            Gets the value for the given key or sets the value if it does not exist.
        set(key, value) -> None:
            Sets the value for the given key.
        get(key) -> Any:
            Gets the value for the given key.
        remove(key) -> Any:
            Removes the value for the given key.
        contains(key) -> bool:
            Determines whether the dictionary contains the given key.
        items() -> List:
            Gets the items in the dictionary.
        keys() -> List:
            Gets the keys in the dictionary.
        values() -> List:
            Gets the values in the dictionary.
        len() -> int:
            Gets the length of the dictionary.
    """
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

class CancellableThread(Thread):
    """
    A thread that can be cancelled.
    This class extends the Thread class and adds a stop event to allow the thread to be cancelled.
    It also provides a cancel callback that can be called when the thread is cancelled.
    Attributes:
        target: The target function to run in the thread.
        args: The arguments to pass to the target function.
        cancel_callback: A callback function to call when the thread is cancelled.
        __stop_event: An event that indicates whether the thread should stop.
    Methods:
        __init__(target=None, args=None, cancel_callback=None) -> None:
            Initializes a new instance of the CancellableThread class.
        cancel() -> None:
            Cancels the thread by setting the stop event and calling the cancel callback if it exists.
        is_cancelled() -> bool:
            Determines whether the thread is cancelled.
            Returns:
                bool: True if the thread is cancelled; otherwise, False.
    """
    def __init__(self, target=None, args=None, cancel_callback=None):
        """
        Initializes a new instance of the CancellableThread class.
        Args:
            target: The target function to run in the thread.
            args: The arguments to pass to the target function.
            cancel_callback: A callback function to call when the thread is cancelled.
        """
        super().__init__(target=target, args=args)
        self.__stop_event = Event()
        self.cancel_callback = cancel_callback

    def cancel(self):
        """
        Cancels the thread.
        This method sets the stop event and calls the cancel callback if it exists.
        """
        if not self.is_cancelled():
            self.__stop_event.set()

            if self.cancel_callback:
                self.cancel_callback()

    def is_cancelled(self):
        """
        Determines whether the thread is cancelled.
        Returns:
            bool: True if the thread is cancelled; otherwise, False.
        """
        return self.__stop_event.is_set()

class QueueProcessor(ContextAwareBase):
    """
    A class that processes a queue of tasks using multiple threads.
    This class uses a PriorityQueue to store tasks and a number of threads to process them concurrently.
    It also provides methods to cancel the threads, clear the queue, and check for exceptions.
    Methods:
        __init__(num_threads: int) -> None:
            Initializes a new instance of the QueueProcessor class.
        process(sync_function: callable, exception_hook: callable = None) -> None:
            Processes the queue using the given synchronous function and an optional exception hook.
        __task_runner(sync_function: callable, workQueue: PriorityQueue) -> None:
            Runs the task runner for processing tasks from the queue.
        cancel_threads() -> None:
            Cancels all threads that are processing tasks in the queue.
        clear() -> None:
            Clears the queue by removing all tasks and marking them as done.
        put(task: callable) -> None:
            Puts the task in the queue for processing.
        empty() -> bool:
            Determines whether the queue is empty.
        has_exceptions() -> bool:
            Determines whether the queue has any exceptions.
    """
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
        self.__lock = Lock()
        self.__cancelled = False

    def process(self, sync_function, exception_hook=None) -> None:
        """
        Processes the queue.
        Args:
            sync_function: The sync function.
            exception_hook: The exception hook.
        """
        self.__cancelled = False
        self.exception_hook = exception_hook

        for i in range(self.num_threads):
            t=CancellableThread(target=self.__task_runner, args=(sync_function, self.workQueue))
            t.name = f"{SyncConstants.THREAD_PREFIX}_{i}"
            t.daemon = True
            t.start() 
            
        self.workQueue.join()

    def __task_runner(self, sync_function, workQueue:PriorityQueue) -> None:
        """
        Runs the task runner.
        Args:
            sync_function: The sync function.
            workQueue: The work queue.
        """
        current_thread = threading.current_thread()

        self.Logger.debug(f"QUEUE PROCESS ({current_thread.name}) STARTING...")

        value = None

        try:
            while not workQueue.empty() and not current_thread.is_cancelled():
                try:
                    value = workQueue.get()
                    sync_function(value)
                finally:
                    workQueue.task_done()
        except Exception as e:
            self.exceptions.append(e)  
            self.Logger.debug(f"QUEUE PROCESS THREAD ERROR ({current_thread.name}): {e}-{traceback.format_exc()}")
            self.Logger.error(f"QUEUE PROCESS THREAD ERROR ({current_thread.name}): {e}")

            if self.exception_hook:
                self.exception_hook((*value, e))

            current_thread.cancel()                
            self.cancel_threads()
        
        self.Logger.debug(f"QUEUE PROCESS ({current_thread.name}) COMPLETED")

    def cancel_threads(self) -> None:
        """
        Cancels all threads.
        This method sets the cancelled flag and cancels all threads that match the thread prefix.
        """
        with self.__lock:
            if not self.__cancelled:
                self.__cancelled = True

                for t in threading.enumerate():
                    if SyncConstants.THREAD_PREFIX in t.name:
                        t.cancel()
                
                self.clear()
          
    def clear(self) -> None:
        """
        Clears the queue.
        This method empties the work queue and clears any tasks that are still in the queue.
        """
        while not self.workQueue.empty():
            self.workQueue.get()
            self.workQueue.task_done()

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
    """
    A class that processes Spark commands using a queue and multiple threads. 
    This class is designed to handle Spark SQL commands in a multi-threaded environment,
    allowing for efficient execution of commands while ensuring thread safety.
    This class extends the ContextAwareBase class and provides methods to process commands,
    delete tables, drop tables, and optimize/vacuum tables.
    It uses the QueueProcessor class to manage the processing of commands in a thread-safe manner.
    Attributes:
        Logger: The logger for logging messages.
        Context: The context for executing SQL commands.
    Methods:
        _processor(cmd: List[CommandSet], num_threads: int = 10) -> None:
            Processes the command using a queue and multiple threads.   
        process_command_list(commands: List[str]) -> None:
            Processes a list of commands.
        delete_from(tables: List[str], schema: str = None) -> None:
            Deletes data from the specified tables in the given schema.
        drop(tables: List[str], schema: str = None) -> None:
            Drops the specified tables in the given schema.
        optimize_vacuum(tables: List[str], schema: str = None) -> None: 
            Optimizes and vacuums the specified tables in the given schema.
        _process_command(value: Tuple) -> None:
            Processes the command from the queue.
            Args:
                value: The value containing the command to process.  
    """
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
            cls.Logger.debug(f"SPARK PROCESSOR - EXECUTE: {c}")
            cls.Context.sql(c)