"Query Caching with Redis"
import dbi, redis, json
import sys

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
            while len(notfound) % 10 != 0:
                notfound += [None]
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
    def run1(self, qname, sql, qkey, tmout):

        if not isinstance(qkey, list) and not isinstance(qkey, tuple):
            qkey = (qkey,)
        
        if tmout <= 0:
            tmout = 0
        else:
            # check cache
            if len(qkey) == 1:
                r = self.__rconn.get(qname + ':' + qkey[0])
            else:
                r = self.__rconn.hget(qname + ':' + qkey[0], repr(qkey[1:]))

            # cache hit?
            if r:
                r = json.loads(r)
                r['_mc'] = 1
                return r

        # cache miss. run query.
        with dbi.DB(self.__dsn) as db:
            # note: this loop will only run once
            for r in db.query(sql, qkey):
                r['_mc'] = 0
                # put in cache
                if tmout:
                    if len(qkey) == 1:
                        self.__rconn.setex(qname + ':' + qkey[0], tmout, json.dumps(r))
                    else:
                        self.__rconn.hset(qname + ':' + qkey[0], repr(qkey[1:]), json.dumps(r))
                        self.__rconn.expire(qname + ':' + qkey[0], tmout)
                    
                return r

        return None


### ------------------------------------------
if __name__ == '__main__':
    if len(sys.argv) != 7:
        sys.exit('Usage: %s uname dbname dbhost dbport rhost rport' % sys.argv[0])

    (uname, dbname, host, port, rhost, rport) = sys.argv[1:]

    rds = redis.StrictRedis(rhost, int(rport))
    dsn = dbi.dsn(dbname, uname, host, port)
    qc = QCache(rds, dsn)

    def test_qrun1():
        qc.invalidate('myquery', 'pg_catalog')
        r = qc.run1('myquery',
                    'select * from pg_tables where schemaname=%s and tablename=%s',
                    ('pg_catalog', 'pg_class'), 60)
        assert r['_mc'] == 0

        r = qc.run1('myquery',
                    'select * from pg_tables where schemaname=%s and tablename=%s',
                    ('pg_catalog', 'pg_type'), 60)
        assert r['_mc'] == 0

        r = qc.run1('myquery',
                    'select * from pg_tables where schemaname=%s and tablename=%s',
                    ('pg_catalog', 'pg_class'), 60)
        assert r['_mc'] == 1

        r = qc.run1('myquery',
                    'select * from pg_tables where schemaname=%s and tablename=%s',
                    ('pg_catalog', 'pg_type'), 60)
        assert r['_mc'] == 1

    def test_qrun10():
        qc.invalidate('anotherquery', 'pg_class')
        qc.invalidate('anotherquery', 'pg_type')
        r = qc.run10('anotherquery',
                     '''select tablename as _qkey, * from pg_tables
                        where schemaname='pg_catalog'
                        and tablename in (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                     ('pg_class', 'pg_type'), 60)
        assert r['pg_class']['_mc'] == 0
        assert r['pg_type']['_mc'] == 0

        r = qc.run10('anotherquery',
                     '''select tablename as _qkey, * from pg_tables
                        where schemaname='pg_catalog'
                        and tablename in (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                     ('pg_class', 'pg_type', 'pg_am'), 60)
        assert r['pg_class']['_mc'] == 1
        assert r['pg_type']['_mc'] == 1
        assert r['pg_am']['_mc'] == 0

        r = qc.run10('anotherquery',
                     '''select tablename as _qkey, * from pg_tables
                        where schemaname='pg_catalog'
                        and tablename in (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                     ('pg_class', 'pg_type', 'pg_am'), 60)
        assert r['pg_class']['_mc'] == 1
        assert r['pg_type']['_mc'] == 1
        assert r['pg_am']['_mc'] == 1

    
    test_qrun1()
    test_qrun1()
    test_qrun10()
