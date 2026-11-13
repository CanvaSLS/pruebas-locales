class MySQLError(Exception):
    pass


class IntegrityError(MySQLError):
    """args = (código MySQL, mensaje): 1062 clave duplicada, 1452 clave foránea."""


class OperationalError(MySQLError):
    pass
