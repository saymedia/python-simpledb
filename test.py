"""
Basic simpledb tests. There's some setup involved in running them since you'll need
an Amazon AWS account that the tests can use. To make this work you'll need a settings.py
file in this directory with the appropriate authorization info. It should look like:

    AWS_KEY = 'XXX'
    AWS_SECRET = 'XXX'

Several test domains will be created during the tests. They should be removed during
test teardown, so they won't stick around long. If any of the domains that the tests
use already exist, an error will be raised and the tests will stop. This is to prevent
any accidental data corruption if there happens to be a name conflict with one of your
existing domains. If this happens you'll need to manually remove the conflicting domain
then re-run the tests.

Note also that tests sometimes fail because of SimpleDB's eventual consistency character-
istics. For example, if you insert a bunch of items and then do a count it may come up
short for some period of time after the inserts. I haven't come up with a good way around
this problem yet. Patches welcome.
"""

import unittest
import simpledb
import simplejson
import settings
from collections import defaultdict


class DomainNameConflict(Exception): pass
class TransactionError(Exception): pass


class SimpleDBTransaction(object):
    """
    A "transaction" simply registers modification events and allows you to
    call rollback or finalize to reverse or commit your changes.
    """

    def __init__(self, sdb, data):
        self.sdb = sdb
        self.data = data
        self.created_domains = set()
        self.modified_items = defaultdict(set)
        self.modified_domains = set()

    def register_modified_item(self, domain, item):
        if isinstance(domain, simpledb.Domain):
            domain_name = domain.name
        else:
            domain_name = domain

        if isinstance(item, simpledb.Item):
            item_name = item.name
        else:
            item_name = item
        
        # We only care about domains we're tracking.
        if domain_name in self.data.keys():
            self.modified_items[domain_name].add(item_name)

    def register_created_domain(self, domain):
        if isinstance(domain, simpledb.Domain):
            domain = domain.name

        if domain in self.data.keys():
            self.modified_domains.add(domain)
        else:
            self.created_domains.add(domain)

    def register_deleted_domain(self, domain):
        if isinstance(domain, simpledb.Domain):
            domain = domain.name
        if domain in self.data.keys():
            self.modified_domains.add(domain)
        elif domain in self.created_domains:
            self.created_domains.remove(domain)
    
    def rollback(self):
        for domain, items in self.modified_items.iteritems():
            if domain in self.created_domains or domain in self.modified_domains:
                # Don't bother rolling back items in domains we're
                # going to delete or recreate from scratch.
                continue

            for item in items:
                if item in self.data[domain]:
                    # If it's in data it was modified, so reverse changes.
                    self.sdb.delete_attributes(domain, item)
                    self.sdb.put_attributes(domain, item, self.data[domain][item])
                else:
                    # Otherwise it was created, so delete it.
                    self.sdb.delete_attributes(domain, item)

        # Delete created domains.
        for domain in self.created_domains:
            del self.sdb[domain]

        # Delete and recreate any modified domains.
        for domain in self.modified_domains:
            self.sdb.create_domain(domain)
            load_data(self.sdb, domain, self.data[domain])


    def finalize(self):
        # Don't need to do anything.
        pass


class SimpleDB(simpledb.SimpleDB):
    """
    Subclass of SimpleDB that registers modifications so we can roll them back after
    each test runs.
    """
    transaction_stack = []
    data = {}

    def start_transaction(self):
        # Transactions need their own non-transaction SimpleDB connections.
        sdb = simpledb.SimpleDB(self.aws_key, self.aws_secret)
        self.transaction_stack.append(SimpleDBTransaction(sdb, self.data))

    def end_transaction(self):
        try:
            transaction = self.transaction_stack.pop()
            transaction.finalize()
        except IndexError:
            raise TransactionError("Tried to end transaction, but no pending transactions exist.")

    def rollback(self):
        try:
            transaction = self.transaction_stack.pop()
            transaction.rollback()
        except IndexError:
            raise TransactionError("Tried to end transaction, but no pending transactions exist.")

    def _register_created_domain(self, domain):
        try:
            self.transaction_stack[-1].register_created_domain(domain)
        except IndexError:
            pass

    def _register_modified_item(self, domain, item):
        try:
            self.transaction_stack[-1].register_modified_item(domain, item)
        except IndexError:
            pass

    def _register_deleted_domain(self, domain):
        try:
            self.transaction_stack[-1].register_deleted_domain(domain)
        except IndexError:
            pass
    
    def create_domain(self, name):
        if self.has_domain(name):
            raise DomainNameConflict("Domain called `%s` already exists! Abort!" % name)
        self._register_created_domain(name)
        return super(SimpleDB, self).create_domain(name)

    def delete_domain(self, domain):
        if isinstance(domain, simpledb.Domain):
            domain_name = domain.name
        else:
            domain_name = domain
        self._register_deleted_domain(domain_name)
        return super(SimpleDB, self).delete_domain(domain)

    def put_attributes(self, domain, item, attributes):
        self._register_modified_item(domain, item)
        return super(SimpleDB, self).put_attributes(domain, item, attributes)

####################################
# Global SimpleDB connection object.
####################################
sdb = SimpleDB(settings.AWS_KEY, settings.AWS_SECRET)


class TransactionTestCase(unittest.TestCase):
    sdb = sdb

    def _pre_setup(self):
        self.data = simplejson.load(open('fixture.json'))
        # Start a transaction
        self.sdb.start_transaction()

    def _post_teardown(self):
        # Reverse the transaction started in _pre_setup
        self.sdb.rollback()

    def __call__(self, result=None):
        """
        Wrapper around default __call__ method to perform common test setup.
        """
        try:
            self._pre_setup()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            import sys
            result.addError(self, sys.exc_info())
            return
        super(TransactionTestCase, self).__call__(result)
        try:
            self._post_teardown()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            import sys
            result.addError(self, sys.exc_info())
            return

class SimpleDBTests(TransactionTestCase):
    def test_count(self):
        self.assertEquals(self.sdb['test_users'].count(), 100)

    def test_create_domain(self):
        domain = self.sdb.create_domain('test_new_domain')
        self.assertTrue(isinstance(domain, simpledb.Domain))
        self.assertTrue(sdb.has_domain('test_new_domain'))

    def test_delete_domain(self):
        domain = self.sdb.create_domain('test_new_domain')
        self.assertTrue(sdb.has_domain('test_new_domain'))
        del self.sdb['test_new_domain']
        self.assertFalse(sdb.has_domain('test_new_domain'))

    def test_simpledb_dictionary(self):
        users = self.sdb['test_users']
        self.assertTrue(isinstance(users, simpledb.Domain))
        self.assertTrue('test_users' in [d.name for d in self.sdb])

    def test_simpledb_domain_dictionary(self):
        users = self.sdb['test_users']
        katie = users['katie']
        self.assertTrue(isinstance(katie, simpledb.Item))
        self.assertEquals(katie['age'], '24')

    def test_domain_setitem(self):
        mike = {'name': 'Mike', 'age': '25', 'location': 'San Francisco, CA'}
        sdb['test_users']['mike'] = mike
        for key, value in sdb['test_users']['mike'].iteritems():
            self.assertEquals(mike[key], value)

    def test_delete(self):
        users = self.sdb['test_users']
        del users['lacy']['age']
        self.assertFalse('age' in users['lacy'].keys())
        del users['lacy']
        self.assertFalse('lacy' in users.item_names())
        del sdb['test_users']
        self.assertFalse('test_users' in [d.name for d in self.sdb])

    def test_select(self):
        users = self.sdb['test_users']
        self.assertEquals(len(users.filter(simpledb.where(name='Fawn') | 
                                           simpledb.where(name='Katie'))), 2)
        k_names = ['Katie', 'Kody', 'Kenya', 'Kim']
        self.assertTrue(users.filter(name__like='K%').count(), len(k_names))
        for item in users.filter(name__like='K%'):
            self.assertTrue(item['name'] in k_names)

    def test_all(self):
        all = self.sdb['test_users'].all()
        self.assertEquals(len(set(i.name for i in all) - set(self.data['test_users'].keys())), 0)

    def test_values(self):
        users = self.sdb['test_users'].filter(age__lt='25').values('name', 'age')
        under_25 = [key for key, value in self.data['test_users'].items() if value['age'] < '25']
        self.assertEquals(len(set(i.name for i in users) - set(under_25)), 0)

    def test_multiple_values(self):
        katie = self.sdb['test_uers']['katie']
        locations = ['San Francisco, CA', 'Centreville, VA']
        katie['location'] = locations
        katie.save()
        katie = self.sdb['test_uers']['katie']
        self.assertTrue(location[0] in katie['location'])
        self.assertTrue(location[1] in katie['location'])
        self.assertTrue(len(katie['location']), 2)


def load_data(sdb, domain, items):
    domain = sdb.create_domain(domain)
    items = [simpledb.Item(sdb, domain, name, attributes) for 
                name, attributes in items.items()]

    # Split into lists of 25 items each (max for BatchPutAttributes).
    batches = [items[i:i+25] for i in xrange(0, len(items), 25)]
    for batch in batches:
        sdb.batch_put_attributes(domain, batch)


if __name__ == '__main__':

    sdb.start_transaction()

    print "Loading fixtures..."

    domains = simplejson.load(open('fixture.json'))
    for domain, items in domains.iteritems():
        load_data(sdb, domain, items)
    sdb.data = domains

    # Run tests.
    unittest.main()

    # Roll back transaction (delete test domains).
    sdb.rollback()
