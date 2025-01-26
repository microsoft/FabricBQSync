from pyspark.sql import SparkSession
from logging import Logger


from FabricSync.BQ.Constants import SyncConstants

class classproperty(property):
  def __get__(self, owner_self, owner_cls):
    """
    Get the value of the property.
    Args:
        owner_self: The owner self.
        owner_cls: The owner class.
    Returns:
        The value of the property.
    """
    return self.fget(owner_cls)

class ContextAwareBase():
    _context:SparkSession = None
    _logger:Logger = None
    
    @classproperty
    def Context(cls) -> SparkSession:
        """
        Gets the Spark context.
        Returns:
            SparkSession: The Spark context.
        """
        if not cls._context:
            cls._context = SparkSession.builder.getOrCreate()

        return cls._context

    @classproperty
    def sync_id(cls) -> str:
        """
        Gets the sync ID.
        Returns:
            str: The sync ID.
        """
        return cls.Context.conf.get(f"{SyncConstants.SPARK_CONF_PREFIX}.name")