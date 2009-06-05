import simplejson

def sdbimport(sdb, domain, items):

    # If the domain doesn't exist, create it.
    if not sdb.has_domain(domain):
        domain = sdb.create_domain(domain)
    else:
        domain = sdb[domain]

    # Load the items.
    for name, value in items.iteritems():
        domain[name] = value


if __name__ == '__main__':
    import os
    import sys
    sys.path.insert(1, os.path.normpath(os.path.join(sys.path[0], '..')))

    import simpledb
    import settings

    if len(sys.argv) != 3:
        print 'Usage: python sdbimport.py <domain> <json_file>'
        sys.exit(1)


    print "Loading..."
    items = simplejson.load(open(sys.argv[2]))

    print "Importing..."
    sdb = simpledb.SimpleDB(settings.AWS_KEY, settings.AWS_SECRET)
    sdbimport(sdb, sys.argv[1], items)

    print "All done."
