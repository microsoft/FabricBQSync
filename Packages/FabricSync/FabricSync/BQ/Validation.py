import re

class SqlValidator():
    """
    A class to validate SQL queries.
    This class provides methods to check if a SQL query is valid and if it contains a predicate.
    It uses regular expressions to perform the validation.
    Attributes:
        __sql_pattern (re.Pattern): A compiled regular expression pattern to match valid SQL queries.
    Methods:
        is_valid(sql: str) -> bool:
            Checks if the SQL query is valid.
        has_predicate(sql: str) -> bool:
            Checks if the SQL query has a predicate.
    """
    __sql_pattern = re.compile(r"^\s*(SELECT|WITH)\s+.*\s+(FROM|INTO|SET)\s+.*", re.IGNORECASE)

    @classmethod
    def is_valid(cls, sql:str) -> bool:
        """
        Check if the SQL query is valid.
        Args:
            sql (str): The SQL query to validate.
        Returns:
            bool: True if the SQL query is valid, False otherwise.
        """
        sql = re.sub(r'[\r\n]', '', sql)
        return (cls.__sql_pattern.match(sql) != None)

    @classmethod
    def has_predicate(cls, sql:str) -> bool:
        """
        Check if the SQL query has a predicate.
        Args:
            sql (str): The SQL query to validate.
        Returns:
            bool: True if the SQL query has a predicate, False otherwise.
        """
        return (re.search(r"\bWHERE\b", sql, re.IGNORECASE) != None)