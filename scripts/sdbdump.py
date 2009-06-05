import simplejson

def sdbdump(sdb, domain):
    items = dict((item.name, dict(item)) for item in sdb[domain])
    return simplejson.dumps(items)


if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(1, os.path.normpath(os.path.join(sys.path[0], '..')))

    import simpledb
    import settings

    if len(sys.argv) != 2:
        print 'Usage: python sdbdump.py <domain>'
        sys.exit(1)

    sdb = simpledb.SimpleDB(settings.AWS_KEY, settings.AWS_SECRET)

    print >>sys.stderr, "Dumping..."
    print sdbdump(sdb, sys.argv[1])
    print >>sys.stderr, "All done..."
