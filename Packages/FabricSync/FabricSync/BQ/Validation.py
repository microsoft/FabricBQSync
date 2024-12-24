import re

class SqlValidator():
    sql_pattern = re.compile(r"^\s*(SELECT|WITH)\s+.*\s+(FROM|INTO|SET)\s+.*", re.IGNORECASE)

    def is_valid(sql):
        return (SqlValidator.sql_pattern.match(sql) != None)

    def has_predicate(sql):
        return (re.search(r"\bWHERE\b", sql, re.IGNORECASE) != None)