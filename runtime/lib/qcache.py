"Query Caching with Redis"
import dbi, redis, json
import sys
import functools

### ------------------------------------------
class QCache:
    "Queries Cache"

    ### ------------------------------------------
    def __init__(self, rds, dsn):
        self.__rconn = rds
        self.__dsn = dsn

    ### ------------------------------------------
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.__rconn = None

    ### ------------------------------------------
    def invalidate(self, qname, qkey):
        "Delete cache record"
        self.__rconn.delete(qname + ':' + qkey)
        
    ### ------------------------------------------
    def run10(self, query_name, sql_query, query_params, timeout):
        """
        Slice query_params in batches having 10 parameters. Batch is filled
        with None if necessary. Execute sql_query string with each batch of
        parameters. Query result must contain _qkey field which defines
        cached value.

        Returns mapping of _qkey keys to corresponding rows. Cache rows by
        query_name string and _qkey.

        timeout integer defines expiration time of cached row, negative
        values bypass caching.
        """
        notfound = []
        out = {}
        
        if timeout <= 0:
            timeout = 0
            notfound += query_params
        else:
            # check cache
            for qkey in query_params:
                rval = self.__rconn.get(query_name + ':' + qkey)
                if rval:
                    rval = json.loads(rval)
                    rval['_mc'] = 1
                    out[qkey] = rval
                else:
                    notfound.append(qkey)

        if notfound:
            # pack it up to multiple of 10
            pad_size = 10 - len(notfound) % 10
            if pad_size < 10:
                notfound += [None] * pad_size
            # split into list of list, each with 10 keys
            keychunk = [notfound[i:i+10] for i in xrange(0, len(notfound), 10)]
            with dbi.DB(self.__dsn) as db:
                for k10 in keychunk:
                    # run the query with 10 keys at a time
                    for row in db.query(sql_query, k10):
                        row['_mc'] = 0
                        qkey = row['_qkey']
                        out[qkey] = row
                        # put in cache
                        if timeout:
                            self.__rconn.setex(query_name + ':' + qkey, timeout,
                                               json.dumps(row))
        return out


    ### ------------------------------------------
    def run1(self, query_name, sql_query, query_params, timeout):
        """Return the first row for sql_query string with query_params
        either from cache or from database with update of the cache.

        query_name string is the prefix of the cache key name

        timeout integer determines expiration of cached value. If timeout is
        negative, then caching is bypassed and data from database is returned.
        """
        if not isinstance(query_params, (list, tuple)):
            query_params = (query_params,)

        key_name = query_name + ':' + query_params[0]
        if len(query_params) == 1:
            read_cache = functools.partial(self.__rconn.get, key_name)
            write_cache = functools.partial(self.__rconn.set, key_name)
        else:
            field_name = repr(query_params[1:])
            read_cache = functools.partial(self.__rconn.hget, key_name,
                field_name)
            write_cache = functools.partial(self.__rconn.hset, key_name,
                field_name)
        
        if timeout <= 0:  # then bypass cache check and query database
            timeout = 0
        else:  # check cache
            rval = read_cache()
            if rval:  # then it is a cache hit
                rval = json.loads(rval)
                rval['_mc'] = 1
                return rval

        # cache miss. run query.
        with dbi.DB(self.__dsn) as db:
            # process result in scope out of with statement,
            # so db connection is returned to the pool asap
            db_result = db.query(sql_query, query_params)

        # FIXME maybe all rows should be cached?
        try:  # get the first row
            rval = db_result[0]
        except IndexError:
            return None
        else:
            rval['_mc'] = 0
            if timeout:  # then put in cache
                write_cache(json.dumps(rval))
                self.__rconn.expire(key_name, timeout)
            return rval

### ------------------------------------------
def test_script():
    "Simple module tests executable from command-line"
    if len(sys.argv) != 7:
        sys.exit(
            'Usage: %s uname dbname dbhost dbport rhost rport' % sys.argv[0])

    (uname, dbname, host, port, rhost, rport) = sys.argv[1:]

    rds = redis.StrictRedis(rhost, int(rport))
    dsn = dbi.dsn(dbname, uname, host, port)
    qc = QCache(rds, dsn)

    def test_qrun1():
        qc.invalidate('myquery', 'pg_catalog')
        res = qc.run1(
            'myquery',
            'select * from pg_tables where schemaname=%s and tablename=%s',
            ('pg_catalog', 'pg_class'), 60)
        assert res['_mc'] == 0

        res = qc.run1(
            'myquery',
            'select * from pg_tables where schemaname=%s and tablename=%s',
            ('pg_catalog', 'pg_type'), 60)
        assert res['_mc'] == 0

        res = qc.run1(
            'myquery',
            'select * from pg_tables where schemaname=%s and tablename=%s',
            ('pg_catalog', 'pg_class'), 60)
        assert res['_mc'] == 1

        res = qc.run1(
            'myquery',
            'select * from pg_tables where schemaname=%s and tablename=%s',
            ('pg_catalog', 'pg_type'), 60)
        assert res['_mc'] == 1

    def test_qrun10():
        qc.invalidate('anotherquery', 'pg_class')
        qc.invalidate('anotherquery', 'pg_type')
        res = qc.run10('anotherquery',
                     '''select tablename as _qkey, * from pg_tables
                        where schemaname='pg_catalog'
                        and tablename in (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                     ('pg_class', 'pg_type'), 60)
        assert res['pg_class']['_mc'] == 0
        assert res['pg_type']['_mc'] == 0

        res = qc.run10('anotherquery',
                     '''select tablename as _qkey, * from pg_tables
                        where schemaname='pg_catalog'
                        and tablename in (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                     ('pg_class', 'pg_type', 'pg_am'), 60)
        assert res['pg_class']['_mc'] == 1
        assert res['pg_type']['_mc'] == 1
        assert res['pg_am']['_mc'] == 0

        res = qc.run10('anotherquery',
                     '''select tablename as _qkey, * from pg_tables
                        where schemaname='pg_catalog'
                        and tablename in (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                     ('pg_class', 'pg_type', 'pg_am'), 60)
        assert res['pg_class']['_mc'] == 1
        assert res['pg_type']['_mc'] == 1
        assert res['pg_am']['_mc'] == 1

    
    test_qrun1()
    test_qrun1()
    test_qrun10()

if __name__ == '__main__':
    test_script()
