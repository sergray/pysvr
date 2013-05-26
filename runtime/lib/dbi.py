"Database Interface"

import psycopg2, psycopg2.extras
import sys, os, time
from collections import defaultdict, deque

### ------------------------------------------
class Error(Exception):
    "Base class for exceptions in dbi module"

### ------------------------------------------
# FIXME redefinition of builtin Python exception RuntimeError
class RuntimeError(Error):
    "Generic error during execution of dbi code"
    def __init__(self, msg):
        super(RuntimeError, self).__init__(msg)

### ------------------------------------------
class ConfigError(RuntimeError):
    "Configuration error in dbi module"
    def __init__(self, msg):
        super(ConfigError, self).__init__(msg)

### ------------------------------------------
# TODO thread safe Pool of connections
class Pool:
    "Pool of database connections"

    tab = defaultdict(deque)  # deque has O(1) append/pop operations
    recycle_after = 60 * 10  # seconds for connections recycling
    
    @staticmethod
    def put(dsn_str, dbconn):
        "Add dbconn to the list of connections for dsn_str"
        dsn_connections = Pool.tab[dsn_str]
        ttl = time.time() + Pool.recycle_after
        dsn_connections.append((dbconn, ttl))

    @staticmethod
    def get(dsn_str):
        """Return psycopg connection for given dsn_str.
        Purge expired connections with O(n), where n=len(Pool.tab)
        """
        while True:
            try:
                dbconn, ttl = Pool.tab[dsn_str].popleft()
            except IndexError:
                return psycopg2.connect(dsn_str)
            else:
                if ttl > time.time():
                    return dbconn
                dbconn.close()
        
### ------------------------------------------
class DB:
    "Database abstraction layer"
    def __init__(self, dsn_str):
        self.__dsn = dsn_str
        self.__conn = None

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def open(self):
        """Open logical database connection using pool of connections. 
        Return self instance."""
        if not self.__conn:
            self.__conn = Pool.get(self.__dsn)
        return self

    def close(self):
        """Close logical database connection, releasing and returning the
        actual connection to the pool"""
        db_con = self.__conn
        self.__conn = None
        if db_con:
            db_con.commit()
            Pool.put(self.__dsn, db_con)

    def query(self, sql_str, params):
        "Execute sql_str query with params and return all fetched results"
        result = None
        cur = self.__conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(sql_str, params)
            result = cur.fetchall()
        except psycopg2.Error:
            self.__conn.rollback()
        else:
            self.__conn.commit()
        finally:
            cur.close()
        return result

    def modify(self, sql_str, params):
        "Execute sql_str query with params and return count of modified rows"
        result = None
        cur = self.__conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(sql_str, params)
            result = cur.rowcount
        except psycopg2.Error:
            self.__conn.rollback()
        else:
            self.__conn.commit()
        finally:
            cur.close()
        return result                
        
    def multi_modify(self, sqlist):
        """Execute sql queries with parameters given in sqlist.
        Return list with counts of modified rows"""
        result_list = []
        cur = self.__conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            for sql, param in sqlist:
                cur.execute(sql, param)
                result_list.append(cur.rowcount)
        except psycopg2.Error:
            self.__conn.rollback()
        else:
            self.__conn.commit()
        finally:
            cur.close()
        return result_list

### ------------------------------------------
def dsn(dbname, uname, host, port='5432'):
    """Return postgres-specific Data-Source Name string, composed from
    the given arguments and environment variable containing password.

    Can raise ConfigError if environment variable does not exist."""

    def env(varname, defval=''):
        "Utility function returning value of environment variable varname"
        retval = os.environ.get(varname, defval)
        if not retval:
            raise ConfigError('env-var %s not set' % varname)
        return retval

    password = env('PG_PASSWORD_%s_%s' % (dbname.upper(), uname.upper()))
    return 'dbname=%s user=%s password=%s host=%s port=%s' % (
        dbname, uname, password, host, port)


### ------------------------------------------
if __name__ == '__main__':
    if len(sys.argv) != 6:
        sys.exit('Usage: %s uname dbname host port {sql|-}' % sys.argv[0])

    (uname, dbname, host, port, sql) = sys.argv[1:]
    if sql == '-':
        sql = sys.stdin.read()
        if not sql:
            sys.exit('Error: cannot read sql from stdin')

    d = dsn(dbname, uname, host, port)

    # this will create one connection
    with DB(d) as db:
        print db.query(sql, ())
        # this will create another connection
        with DB(d) as db:
            for i in db.query(sql, ()):
                print i

    # reuse one of the connections above
    with DB(d) as db:
        for i in db.query(sql, ()):
            print i
