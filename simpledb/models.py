import simpledb
import datetime


__all__ = ['FieldError', 'Field', 'NumberField', 'BooleanField', 'DateTimeField', 'Manager', 'Model']


class FieldError(Exception): pass


class Field(object):
    name = False

    def __init__(self, default=None, required=False):
        self.default = default
        self.required = required

    def install(self, name, cls):
        default = self.default
        # If the default argument is a callable, call it.
        if callable(default):
            default = default()
        setattr(cls, name, default)

    def decode(self, value):
        """Decodes an object from the datastore into a python object."""
        return value

    def encode(self, value):
        """Encodes a python object into a value suitable for the backend datastore."""
        return value


class ItemName(Field):
    """The item's name. Must be a UTF8 string."""
    name = True


class NumberField(Field):
    def __init__(self, padding=0, offset=0, precision=0, **kwargs):
        self.padding = padding
        self.offset = offset
        self.precision = precision
        super(NumberField, self).__init__(**kwargs)

    def encode(self, value):
        """
        Converts a python number into a padded string that is suitable for storage
        in Amazon SimpleDB and can be sorted lexicographically.

        Numbers are shifted by an offset so that negative numbers sort correctly. Once
        shifted, they are converted to zero padded strings.
        """
        padding = self.padding
        if self.precision > 0 and self.padding > 0:
            # Padding shouldn't include decimal digits or the decimal point.
            padding += self.precision + 1
        return ('%%0%d.%df' % (padding, self.precision)) % (value + self.offset)

    def decode(self, value):
        """
        Decoding converts a string into a numerical type then shifts it by the
        offset.
        """
        return float(value) - self.offset


class BooleanField(Field):
    def encode(self, value):
        """
        Converts a python boolean into a string '1'/'0' for storage in SimpleDB.
        """
        return ('0','1')[value]

    def decode(self, value):
        """
        Converts an encoded string '1'/'0' into a python boolean object.
        """
        return {'0': False, '1': True}[value]


class DateTimeField(Field):
    def __init__(self, format='%Y-%m-%dT%H:%M:%S', **kwargs):
        self.format = format
        super(DateTimeField, self).__init__(**kwargs)

    def encode(self, value):
        """
        Converts a python datetime object to a string format controlled by the
        `format` attribute. The default format is ISO 8601, which supports
        lexicographical order comparisons.
        """
        return value.strftime(self.format)
    
    def decode(self, value):
        """
        Decodes a string representation of a date and time into a python
        datetime object.
        """
        return datetime.datetime.strptime(value, self.format)


class FieldEncoder(simpledb.AttributeEncoder):
    def __init__(self, fields):
        self.fields = fields

    def encode(self, domain, attribute, value):
        try:
            field = self.fields[attribute]
        except KeyError:
            return value
        else:
            return field.encode(value)

    def decode(self, domain, attribute, value):
        try:
            field = self.fields[attribute]
        except KeyError:
            return value
        else:
            return field.decode(value)


class Query(simpledb.Query):
    def values(self, *fields):
        # If you ask for specific values return a simpledb.Item instead of the Model
        q = self._clone(klass=simpledb.Query)
        q.fields = fields
        return q

    def _get_results(self):
        if self._result_cache is None:
            self._result_cache = [self.domain.model.from_item(item) for item in 
                                  self.domain.select(self.to_expression())]
        return self._result_cache


class Manager(object):
    # Tracks each time a Manager instance is created. Used to retain order.
    creation_counter = 0

    def __init__(self):
        self._set_creation_counter()
        self.model = None

    def install(self, name, model):
        self.model = model
        setattr(model, name, ManagerDescriptor(self))
        if not getattr(model, '_default_manager', None) or self.creation_counter < model._default_manager.creation_counter:
            model._default_manager = self

    def _set_creation_counter(self):
        """
        Sets the creation counter value for this instance and increments the
        class-level copy.
        """
        self.creation_counter = Manager.creation_counter
        Manager.creation_counter += 1

    def filter(self, *args, **kwargs):
        return self._get_query().filter(*args, **kwargs)

    def all(self):
        return self._get_query()

    def count(self):
        return self._get_query().count()

    def values(self, *args):
        return self._get_query().values(*args)

    def item_names(self):
        return self._get_query().item_names()

    def get(self, name):
        return self.model.from_item(self.model.Meta.domain.get(name))

    def _get_query(self):
        return Query(self.model.Meta.domain)


class ManagerDescriptor(object):
    # This class ensures managers aren't accessible via model instances.
    # For example, Poll.objects works, but poll_obj.objects raises AttributeError.
    def __init__(self, manager):
        self.manager = manager

    def __get__(self, instance, type=None):
        if instance != None:
            raise AttributeError("Manager isn't accessible via %s instances" % type.__name__)
        return self.manager


class ModelMetaclass(type):
    """
    Metaclass for `simpledb.models.Model` instances. Installs 
    `simpledb.models.Field` instances declared as attributes of the
    new class.
    """

    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, ModelMetaclass)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super(ModelMetaclass, cls).__new__(cls, name, bases, attrs)
        fields = {}

        for base in bases:
            if isinstance(base, ModelMetaclass) and hasattr(base, 'fields'):
                fields.update(base.fields)

        new_fields = {}
        managers = {}

        # Move all the class's attributes that are Fields to the fields set.
        for attrname, field in attrs.items():
            if isinstance(field, Field):
                new_fields[attrname] = field
                if field.name:
                    # Add _name_field attr so we know what the key is
                    if '_name_field' in attrs:
                        raise FieldError("Multiple key fields defined for model '%s'" % name)
                    attrs['_name_field'] = attrname
            elif attrname in fields:
                # Throw out any parent fields that the subclass defined as
                # something other than a field
                del fields[attrname]

            # Track managers
            if isinstance(field, Manager):
                managers[attrname] = field

        fields.update(new_fields)
        attrs['fields'] = fields
        new_cls = super(ModelMetaclass, cls).__new__(cls, name, bases, attrs)

        for field, value in new_fields.items():
            new_cls.add_to_class(field, value)

        if not managers:
            managers['objects'] = Manager()

        for field, value in managers.items():
            new_cls.add_to_class(field, value)

        if hasattr(new_cls, 'Meta'):
            # If the new class's Meta.domain attribute is a string turn it into
            # a simpledb.Domain instance.
            if isinstance(new_cls.Meta.domain, basestring):
                new_cls.Meta.domain = simpledb.Domain(new_cls.Meta.domain, new_cls.Meta.connection)
            # Install a reference to the new model class on the Meta.domain so
            # Query can use it.
            # TODO: Should we be using weakref here? Not sure it matters since it's 
            # a class (global) that's long lived anyways.
            new_cls.Meta.domain.model = new_cls

            # Set the connection object's AttributeEncoder
            new_cls.Meta.connection.encoder = FieldEncoder(fields)

        return new_cls

    def add_to_class(cls, name, value):
        if hasattr(value, 'install'):
            value.install(name, cls)
        else:
            setattr(cls, name, value)


class Model(object):

    __metaclass__ = ModelMetaclass

    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            setattr(self, name, value)
        self._item = None

    def _get_name(self):
        return getattr(self, self._name_field)

    def save(self):
        if self._item is None:
            self._item = simpledb.Item(self.Meta.connection, self.Meta.domain, self._get_name())
        for name, field in self.fields.items():
            if field.name:
                continue
            value = getattr(self, name)
            if value is None:
                if field.required:
                    raise FieldError("Missing required field '%s'" % name)
                else:
                    del self._item[name]
                    continue
            self._item[name] = getattr(self, name)
        self._item.save()

    def delete(self):
        del self.Meta.domain[self._get_name()]

    @classmethod
    def from_item(cls, item):
        obj = cls()
        obj._item = item
        for name, field in obj.fields.items():
            if name in obj._item:
                setattr(obj, name, obj._item[name])
        setattr(obj, obj._name_field, obj._item.name)
        return obj
