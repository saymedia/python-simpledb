from django.core.management.base import BaseCommand, CommandError
from django.db.models.loading import AppCache
from django.conf import settings

import simpledb

class Command(BaseCommand):
    help = ("Sync all of the SimpleDB domains.")

    def handle(self, *args, **options):
        apps = AppCache()
        check = []
        for module in apps.get_apps():
            for d in module.__dict__:
                ref = getattr(module, d)
                if isinstance(ref, simpledb.models.ModelMetaclass):
                    domain = ref.Meta.domain.name
                    if domain not in check:
                        check.append(domain)

        sdb = simpledb.SimpleDB(settings.AWS_KEY, settings.AWS_SECRET)
        domains = [d.name for d in list(sdb)]
        for c in check:
            if c not in domains:
                sdb.create_domain(c)
                print "Creating domain %s ..." % c
