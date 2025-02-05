import re

class SqlValidator():
    """
    A class to validate SQL queries.
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