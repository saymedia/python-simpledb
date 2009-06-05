import simplejson
from sdbdump import sdbdump
from sdbimport import sdbimport

def sdbcopy(sdb, from_domain, to_domain):
    json = sdbdump(sdb, from_domain)
    sdbimport(sdb, to_domain, simplejson.loads(json))

if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(1, os.path.normpath(os.path.join(sys.path[0], '..')))

    import simpledb
    import settings

    if len(sys.argv) != 3:
        print 'Usage: python sdbcopy.py from_domain to_domain'
        sys.exit(1)

    sdb = simpledb.SimpleDB(settings.AWS_KEY, settings.AWS_SECRET)

    print >>sys.stderr, "Copying..."
    sdbcopy(sdb, sys.argv[1], sys.argv[2])
    print >>sys.stderr, "All done..."
