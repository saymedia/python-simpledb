import httplib2
import urlparse
import urllib
import time
import hmac
import base64
try:
    import xml.etree.ElementTree as ET
except ImportError:
    import elementtree.ElementTree as ET
from UserDict import DictMixin


__all__ = ['SimpleDB', 'Domain', 'Item', 'AttributeEncoder', 'where', 'every', 'item_name', 'SimpleDBError', 'ItemDoesNotExist']


QUERY_OPERATORS = {
    # Note that `is null`, `is not null` and `every` are handled specially by using
    # attr__eq = None, attr__noteq = None, and every(), respectively.
    'eq': '=',              # equals
    'noteq': '!=',          # not equals
    'gt': '>',              # greater than
    'gte': '>=',            # greater than or equals
    'lt': '<',              # less than
    'lte': '<=',            # less than or equals
    'like': 'like',         # contains, works with `%` globs: '%string' or 'string%'
    'notlike': 'not like',  # doesn't contain
    'btwn': 'between',      # falls within range (inclusive)
    'in': 'in',             # equal to one of
}


RESERVED_KEYWORDS = (
    'OR', 'AND', 'NOT', 'FROM', 'WHERE', 'SELECT', 'LIKE', 'NULL', 'IS', 'ORDER',
    'BY', 'ASC', 'DESC', 'IN', 'BETWEEN', 'INTERSECTION', 'LIMIT', 'EVERY',
)


class SimpleDBError(Exception): pass
class ItemDoesNotExist(Exception): pass


def generate_timestamp():
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())


def _utf8_str(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    else:
        return str(s)


def escape(s):
    return urllib.quote(s, safe='-_~')


def urlencode(d):
    if isinstance(d, dict):
        d = d.iteritems()
    return '&'.join(['%s=%s' % (escape(k), escape(v)) for k, v in d])


class SignatureMethod(object):

    @property
    def name(self):
        raise NotImplementedError

    def build_signature_base_string(self, request):
        sig = '\n'.join((
            request.get_normalized_http_method(),
            request.get_normalized_http_host(),
            request.get_normalized_http_path(),
            request.get_normalized_parameters(),
        ))
        return sig

    def build_signature(self, request, aws_secret):
        raise NotImplementedError


class SignatureMethod_HMAC_SHA1(SignatureMethod):
    name = 'HmacSHA1'
    version = '2'

    def build_signature(self, request, aws_secret):
        base = self.build_signature_base_string(request)
        try:
            import hashlib # 2.5
            hashed = hmac.new(aws_secret, base, hashlib.sha1)
        except ImportError:
            import sha # deprecated
            hashed = hmac.new(aws_secret, base, sha)
        return base64.b64encode(hashed.digest())


class SignatureMethod_HMAC_SHA256(SignatureMethod):
    name = 'HmacSHA256'
    version = '2'

    def build_signature(self, request, aws_secret):
        import hashlib
        base = self.build_signature_base_string(request)
        hashed = hmac.new(aws_secret, base, hashlib.sha256)
        return base64.b64encode(hashed.digest())


class Response(object):
    def __init__(self, response, content, request_id, usage):
        self.response = response
        self.content = content
        self.request_id = request_id
        self.usage = usage


class Request(object):
    def __init__(self, method, url, parameters=None):
        self.method = method
        self.url = url
        self.parameters = parameters or {}

    def set_parameter(self, name, value):
        self.parameters[name] = value

    def get_parameter(self, parameter):
        try:
            return self.parameters[parameter]
        except KeyError:
            raise SimpleDBError('Parameter not found: %s' % parameter)

    def to_postdata(self):
        return urlencode([(_utf8_str(k), _utf8_str(v)) for k, v in self.parameters.iteritems()])

    def get_normalized_parameters(self):
        """
        Returns a list consisting of all the parameters required in the
        signature in the proper order.

        """
        return urlencode([(_utf8_str(k), _utf8_str(v)) for k, v in 
                            sorted(self.parameters.iteritems()) 
                            if k != 'Signature'])

    def get_normalized_http_method(self):
        return self.method.upper()

    def get_normalized_http_path(self):
        parts = urlparse.urlparse(self.url)
        if not parts[2]:
            # For an empty path use '/'
            return '/'
        return parts[2]

    def get_normalized_http_host(self):
        parts = urlparse.urlparse(self.url)
        return parts[1].lower()

    def sign_request(self, signature_method, aws_key, aws_secret):
        self.set_parameter('AWSAccessKeyId', aws_key)
        self.set_parameter('SignatureVersion', signature_method.version)
        self.set_parameter('SignatureMethod', signature_method.name)
        self.set_parameter('Timestamp', generate_timestamp())
        self.set_parameter('Signature', signature_method.build_signature(self, aws_secret))


class AttributeEncoder(object):
    """
    AttributeEncoder converts Python objects into UTF8 strings suitable for
    storage in SimpleDB.
    """

    def encode(self, domain, attribute, value):
        return value

    def decode(self, domain, attribute, value):
        return value


class NumberEncoder(object):
    def encode(self, domain, attribute, value):
        if isinstance(value, int):
            return str(value + 10000)
        return value

    def decode(self, domain, attribute, value):
        if value.isdigit():
            return int(value) - 10000
        return value


class SimpleDB(object):
    """Represents a connection to Amazon SimpleDB."""

    ns = 'http://sdb.amazonaws.com/doc/2009-04-15/'
    service_version = '2009-04-15'
    try:
        import hashlib # 2.5+
        signature_method = SignatureMethod_HMAC_SHA256
    except ImportError:
        signature_method = SignatureMethod_HMAC_SHA1


    def __init__(self, aws_access_key, aws_secret_access_key, db='sdb.amazonaws.com', 
                 secure=True, encoder=AttributeEncoder()):
        """
        Use your `aws_access_key` and `aws_secret_access_key` to create a connection to
        Amazon SimpleDB.

        SimpleDB requests are directed to the host specified by `db`, which defaults to
        ``sdb.amazonaws.com``.

        The optional `secure` argument specifies whether HTTPS should be used. The 
        default value is ``True``.
        """

        self.aws_key = aws_access_key
        self.aws_secret = aws_secret_access_key
        if secure:
            self.scheme = 'https'
        else:
            self.scheme = 'http'
        self.db = db
        self.http = httplib2.Http()
        self.encoder = encoder

    def _make_request(self, request):
        headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8', 
                   'host': self.db}
        request.set_parameter('Version', self.service_version)
        request.sign_request(self.signature_method(), self.aws_key, self.aws_secret)
        response, content = self.http.request(request.url, request.method, headers=headers, body=request.to_postdata())
        e = ET.fromstring(content)

        error = e.find('Errors/Error')
        if error:
            raise SimpleDBError(error.find('Message').text)

        meta = e.find('{%s}ResponseMetadata' % self.ns)
        request_id = meta.find('{%s}RequestId' % self.ns).text
        usage = meta.find('{%s}BoxUsage' % self.ns).text

        return Response(response, content, request_id, usage)

    def _sdb_url(self):
        return urlparse.urlunparse((self.scheme, self.db, '', '', '', ''))

    def create_domain(self, name):
        """
        Creates a new domain.

        The domain `name` argument must be a string, and must be unique among 
        the domains associated with your AWS Access Key. The CreateDomain operation 
        may take 10 or more seconds to complete. By default, you can create up to 
        100 domains per account.

        Returns the newly created `Domain` object.
        """

        data = {
            'Action': 'CreateDomain',
            'DomainName': name,
        }
        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)
        return Domain(name, self)
    
    def delete_domain(self, domain):
        """
        Deletes a domain. Any items (and their attributes) in the domain are
        deleted as well. The DeleteDomain operation may take 10 or more seconds
        to complete.

        The `domain` argument can be a string representing the name of the 
        domain, or a `Domain` object.
        """

        if isinstance(domain, Domain):
            domain = domain.name
        data = {
            'Action': 'DeleteDomain',
            'DomainName': domain,
        }
        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)

    def _list_domains(self):
        # Generator that yields each domain associated with the AWS Access Key.
        data = {
            'Action': 'ListDomains',
            'MaxNumberOfDomains': '100',
        }

        while True:
            request = Request("POST", self._sdb_url(), data)
            response = self._make_request(request)

            e = ET.fromstring(response.content)
            domain_result = e.find('{%s}ListDomainsResult' % self.ns)
            if domain_result:
                domain_names = domain_result.findall('{%s}DomainName' % self.ns)
                for domain in domain_names:
                    yield Domain(domain.text, self)

                # SimpleDB will return a max of 100 domains per request, and
                # will return a NextToken if there are more.
                next_token = domain_result.find('{%s}NextToken' % self.ns)
                if next_token is None:
                    break
                data['NextToken'] = next_token.text
            else:
                break

    def list_domains(self):
        """
        Lists all domains associated with your AWS Access Key.
        """
        return list(self._list_domains())

    def has_domain(self, domain):
        if isinstance(domain, Domain):
            domain = domain.name
        return domain in [d.name for d in self.list_domains()]

    def get_domain_metadata(self, domain):
        """
        Returns information about the domain. Includes when the domain was
        created, the number of items and attributes, and the size of attribute
        names and values.

        The `domain` argument can be a string representing the name of the
        domain or a `Domain` object.
        """
        if isinstance(domain, Domain):
            domain = domain.name
        data = {
            'Action': 'DomainMetadata',
            'DomainName': domain,
        }
        request = Request("POST", self._sdb_url(), data)
        response = self._make_request(request)

        e = ET.fromstring(response.content)
        metadata = {}
        metadata_result = e.find('{%s}DomainMetadataResult' % self.ns)
        if metadata_result is not None:
            for child in metadata_result.getchildren():
                tag, text = child.tag, child.text
                if tag.startswith('{%s}' % self.ns):
                    tag = tag[42:] # Die ElementTree namespaces, die!
                metadata[tag] = text
        return metadata

    def put_attributes(self, domain, item, attributes):
        """
        Creates or replaces attributes in an item.

        The `domain` and `item` arguments can be strings representing the
        domain and item names, or `Domain` and `Item` objects, respectively.

        The `attributes` argument should be a dictionary containing the
        attribute names -> values that you would like stored for the 
        specified `item` or a list of (<attribute name>, <value>, <replace>)
        tuples.

        By default, attributes are "replaced". This causes new attribute values to 
        overwrite existing values. For example, if an item has the attributes 
        ('a', '1'), ('b', '2') and ('b', '3') and you call put_attributes using 
        the attributes ('b', '4'), the final attributes of the item are changed to 
        ('a', '1') and ('b', '4'), which replaces the previous value of the 'b' 
        attribute with the new value.

        SimpleDB allows you to associate multiple values with a single attribute.
        If an attribute has multiple values, it will be coalesced into a single
        list. Likewise, if you'd like to store multiple values for a single
        attribute, you should pass in a list value to this method.
        """

        if isinstance(domain, Domain):
            domain = domain.name
        if isinstance(item, Item):
            item = item.name
        if hasattr(attributes, 'items'):
            # Normalize attributes into a list of tuples.
            attributes = attributes.items()

        data = {
            'Action': 'PutAttributes',
            'DomainName': domain,
            'ItemName': item,
        }
        idx = 0
        for attribute in attributes:
            name = attribute[0]
            values = attribute[1]
            if not hasattr(values, '__iter__') or isinstance(values, basestring):
                values = [values]
            for value in values:
                value = self.encoder.encode(domain, name, value)
                data['Attribute.%s.Name' % idx] = name
                data['Attribute.%s.Value' % idx] = value
                if len(attribute) == 2 or attribute[2]:
                    data['Attribute.%s.Replace' % idx] = 'true'
                idx += 1
        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)

    def batch_put_attributes(self, domain, items, replace=True):
        """
        Performs multiple PutAttribute operations in a single call. This yields
        savings in round trips and latencies and enables SimpleDB to optimize
        your request, which generally yields better throughput.

        The `domain` argument can be a string representing the name of the
        domain or a `Domain` object.

        The `items` argument should be a list of `Item` objects or a list of 
        (<item name>, <attributes>) tuples. See the documentation for the
        put_attribute method for a description of how attributes should be 
        represented.
        """
        
        if isinstance(domain, Domain):
            domain = domain.name

        data = {
            'Action': 'BatchPutAttributes',
            'DomainName': domain,
        }
        for item_idx, item in enumerate(items):
            if isinstance(item, Item):
                item = [item.name, item.attributes]
            item = list(item)
            if hasattr(item[1], 'items'):
                # Normalize attributes into a list of tuples.
                item[1] = item[1].items()

            data['Item.%s.ItemName' % item_idx] = item[0]
            attr_idx = 0
            for attribute in item[1]:
                name = attribute[0]
                values = attribute[1]
                if isinstance(values, basestring):
                    values = [values]
                for value in values:
                    value = self.encoder.encode(domain, name, value)
                    data['Item.%s.Attribute.%s.Name' % (item_idx, attr_idx)] = name
                    data['Item.%s.Attribute.%s.Value' % (item_idx, attr_idx)] = value
                    if len(attribute) == 2 or attribute[2]:
                        data['Item.%s.Attribute.%s.Replace' % (item_idx, attr_idx)] = 'true'
                    attr_idx += 1
        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)

    def delete_attributes(self, domain, item, attributes=None):
        """
        Deletes one or more attributes associated with an item. If all attributes of
        an item are deleted, the item is deleted.

        If the optional parameter `attributes` is not provided, all items are deleted.
        """
        if isinstance(domain, Domain):
            domain = domain.name
        if isinstance(item, Item):
            item = item.name
        if attributes is None:
            attributes = {}

        data = {
            'Action': 'DeleteAttributes',
            'DomainName': domain,
            'ItemName': item,
        }
        for i, (name, value) in enumerate(attributes.iteritems()):
            value = self.encoder.encode(domain, name, value)
            data['Attribute.%s.Name' % i] = name
            data['Attribute.%s.Value' % i] = value
        request = Request("POST", self._sdb_url(), data)
        self._make_request(request)

    def get_attributes(self, domain, item, attributes=None):
        """
        Returns all of the attributes associated with the item.
        
        The returned attributes can be limited by passing a list of attribute
        names in the optional `attributes` argument.

        If the item does not exist, an empty set is returned. An error is not
        raised because SimpleDB provides no guarantee that the item does not
        exist on another replica. In other words, if you fetch attributes that 
        should exist, but get an empty set, you may have better luck if you try
        again in a few hundred milliseconds.
        """
        if isinstance(domain, Domain):
            domain = domain.name
        if isinstance(item, Item):
            item = item.name

        data = {
            'Action': 'GetAttributes',
            'DomainName': domain,
            'ItemName': item,
        }
        if attributes:
            for i, attr in enumerate(attributes):
                data['AttributeName.%s' % i] = attr
        request = Request("POST", self._sdb_url(), data)
        response = self._make_request(request)

        e = ET.fromstring(response.content)
        attributes = dict.fromkeys(attributes or [])
        attr_node = e.find('{%s}GetAttributesResult' % self.ns)
        if attr_node:
            attributes.update(self._parse_attributes(domain, attr_node))
        return attributes

    def _parse_attributes(self, domain, attribute_node):
        # attribute_node should be an ElementTree node containing Attribute
        # child elements.
        attributes = {}
        for attribute in attribute_node.findall('{%s}Attribute' % self.ns):
            name = attribute.find('{%s}Name' % self.ns).text
            value = attribute.find('{%s}Value' % self.ns).text
            value = self.encoder.decode(domain, name, value)
            if name in attributes:
                if isinstance(attributes[name], list):
                    attributes[name].append(value)
                else:
                    attributes[name] = [attributes[name], value]
            else:
                attributes[name] = value
        return attributes

    def _select(self, domain, expression):
        if not isinstance(domain, Domain):
            domain = Domain(domain, self)
        data = {
            'Action': 'Select',
            'SelectExpression': expression,
        }

        while True:
            request = Request("POST", self._sdb_url(), data)
            response = self._make_request(request)

            e = ET.fromstring(response.content)
            item_node = e.find('{%s}SelectResult' % self.ns)
            if item_node is not None:
                for item in item_node.findall('{%s}Item' % self.ns):
                    name = item.findtext('{%s}Name' % self.ns)
                    attributes = self._parse_attributes(domain, item)
                    yield Item(self, domain, name, attributes)

                # SimpleDB will return a max of 100 items per request, and
                # will return a NextToken if there are more.
                next_token = item_node.find('{%s}NextToken' % self.ns)
                if next_token is None:
                    break
                data['NextToken'] = next_token.text
            else:
                break

    def select(self, domain, expression):
        return list(self._select(domain, expression))

    def __iter__(self):
        return self._list_domains()

    def __getitem__(self, name):
        # TODO: Check if it's a valid domain
        return Domain(name, self)

    def __delitem__(self, name):
        self.delete_domain(name)


class where(object):
    """
    Encapsulate where clause as objects that can be combined logically using
    & and |.
    """

    # Connection types
    AND = 'AND'
    OR = 'OR'
    default = AND

    def __init__(self, *args, **query):
        self.connector = self.default
        self.children = []
        self.children.extend(args)
        for key, value in query.iteritems():
            if '__' in key:
                parts = key.split('__')
                if len(parts) != 2:
                    raise ValueError("Filter arguments should be of the form "
                        "`field__operation`")
                field, operation = parts
            else:
                field, operation = key, 'eq'

            if operation not in QUERY_OPERATORS:
                raise ValueError('%s is not a valid query operation' % (operation,))
            self.children.append((field, operation, value))

    def __len__(self):
        return len(self.children)

    def to_expression(self, encoder):
        """
        Returns the query expression for the where clause. Returns an empty
        string if the node is empty.
        """
        where = []
        for child in self.children:
            if hasattr(child, 'to_expression'):
                expr = child.to_expression(encoder)
                if expr:
                    where.append('(%s)' % expr)
            else:
                field, operation, value = child
                operator = QUERY_OPERATORS[operation]
                if hasattr(self, '_make_%s_condition' % operation):
                    expr = getattr(self, '_make_%s_condition' % operation)(field, operator, value, encoder)
                else:
                    expr = self._make_condition(field, operator, value, encoder)
                where.append(expr)
        conn_str = ' %s ' % self.connector
        return conn_str.join(where)

    def add(self, other, conn):
        """
        Adds a new clause to the where statement. If the connector type is the
        same as the root's current connector type, the clause is added to the
        first level. Otherwise, the whole tree is pushed down one level and a
        new root connector is created, connecting the existing clauses and the
        new clause.
        """
        if other in self.children and conn == self.connector:
            return
        if len(self.children) < 2:
            self.connector = conn
        if self.connector == conn:
            if isinstance(other, where) and (other.connector == conn or 
                    len(other) <= 1):
                self.children.extend(other.children)
            else:
                self.children.append(other)
        else:
            obj = self._clone()
            self.connector = conn
            self.children = [obj, other]

    def _make_condition(self, attribute, operation, value, encoder):
        value = encoder(attribute, value)
        return "%s %s '%s'" % (self._quote_attribute(attribute), 
                                    operation, self._quote(value))

    def _make_eq_condition(self, attribute, operation, value, encoder):
        value = encoder(attribute, value)
        if value is None:
            return '%s IS NULL' % attribute
        return self._make_condition(attribute, operation, value, encoder)

    def _make_noteq_condition(self, attribute, operation, value, encoder):
        value = encoder(attribute, value)
        if value is None:
            return '%s IS NOT NULL' % attribute
        return self._make_condition(attribute, operation, value, encoder)

    def _make_in_condition(self, attribute, operation, value, encoder):
        value = [encoder(attribute, v) for v in value]
        return '%s %s(%s)' % (attribute, operation, 
                              ', '.join("'%s'" % self._quote(v) for v in value))

    def _make_btwn_condition(self, attribute, operation, value, encoder):
        if len(value) != 2:
            raise ValueError('Invalid value `%s` for between clause. Requires two item list.' % value)
        value = [encoder(attribute, value[0]), encoder(attribute, value[1])]
        return "%s between '%s' and '%s'" % (attribute, self._quote(value[0]), self._quote(value[1]))

    def _quote_attribute(self, s):
        if s.upper() in RESERVED_KEYWORDS:
            return '`%s`' % s
        return s

    def _quote(self, s):
        return s.replace('\'', '\'\'')

    def _clone(self, klass=None, **kwargs):
        if klass is None:
            klass = self.__class__
        obj = klass()
        obj.connector = self.connector
        obj.children = self.children[:]
        return obj

    def _combine(self, other, conn):
        if not isinstance(other, where):
            raise TypeError(other)
        obj = self._clone()
        obj.add(other, conn)
        return obj
    
    def __or__(self, other):
        return self._combine(other, self.OR)

    def __and__(self, other):
        return self._combine(other, self.AND)


class every(where):
    """
    Encapsulates a where clause and uses the every() operator which,
    for multi-valued attributes, checks that every attribute satisfies
    the constraint.
    """
    def _every(self, attribute):
        return "every(%s)" % self._quote_attribute(attribute)

    def _make_condition(self, attribute, operation, value, encoder):
        return super(every, self)._make_condition(self._every(attribute), operation, value, encoder)

    def _make_eq_condition(self, attribute, operation, value, encoder):
        if value is None:
            attribute = self._every(attribute)
        return super(every, self)._make_eq_condition(attribute, operation, value, encoder)

    def _make_noteq_condition(self, attribute, operation, value, encoder):
        if value is None:
            attribute = self._every(attribute)
        return super(every, self)._make_noteq_condition(attribute, operation, value, encoder)

    def _make_in_condition(self, attribute, operation, value, encoder):
        return super(every, self)._make_in_condition(self._every(attribute), operation, value, encoder)

    def _make_btwn_condition(self, attribute, operation, value, encoder):
        return super(every, self)._make_btwn_condition(self._every(attribute), operation, value, encoder)


class item_name(where):
    """
    Encapsulates a where clause that filters based on item names.
    """
    def __init__(self, *equals, **query):
        self.connector = self.default
        self.children = []
        for equal in equals:
            self.children.append(('itemName()', 'eq', equal))
        for operation, value in query.iteritems():
            self.children.append(('itemName()', operation, value))


class Query(object):

    DESCENDING = 'DESC'
    ASCENDING = 'ASC'

    def __init__(self, domain):
        self.domain = domain
        self.where = where()
        self.fields = []
        self.limit = None
        self.order = None
        self._result_cache = None

    def __iter__(self):
        return iter(self._get_results())

    def __len__(self):
        return len(self._get_results())

    def __repr__(self):
        return repr(list(self))

    def __getitem__(self, k):
        if not isinstance(k, (slice, int, long)):
            raise TypeError
        if self._result_cache:
            return self._result_cache[k]

        q = self._clone()
        if isinstance(k, slice) and k.stop >= 0:
            q.limit = k.stop + 1
        elif k >= 0:
            q.limit = k + 1
        return list(q)[k]

    def all(self):
        return self._clone()

    def limit(self, limit):
        q = self._clone()
        q.limit = limit
        return q

    def filter(self, *args, **kwargs):
        q = self._clone()
        q.where = self.where & where(*args, **kwargs)
        return q

    def values(self, *fields):
        q = self._clone()
        q.fields = fields
        return q

    def item_names(self):
        q = self._clone(klass=ItemNameQuery)
        return q

    def count(self):
        if self._result_cache:
            return len(self._result_cache)
        q = self._clone()
        q.fields = ['count(*)']
        return int(list(q)[0]['Count'])

    def order_by(self, field):
        q = self._clone()
        if field[0] == '-':
            field = field[1:]
            q.order = (field, self.DESCENDING)
        else:
            q.order = (field, self.ASCENDING)
        return q

    def get(self, name):
        q = self._clone()
        q = q.filter(item_name(name))
        if len(q) < 1:
            raise ItemDoesNotExist(name)
        return q[0]

    def to_expression(self):
        """
        Creates the query expression for this query. Returns the expression 
        string.
        """

        # Used to encode attribute values in `where` instances, since they
        # don't know the Domain they're operating on.
        encoder = lambda a, v: self.domain._encode(a, v)

        if self.fields:
            output_list = self.fields
        else:
            output_list = ['*']
        stmt = ['SELECT', ', '.join(output_list), 'FROM', '`%s`' % self.domain.name]
        if len(self.where):
            stmt.extend(['WHERE', self.where.to_expression(encoder)])
        if self.order is not None:
            stmt.append('ORDER BY')
            stmt.extend(self.order)
        if self.limit is not None:
            stmt.append('LIMIT %s' % self.limit)
        return ' '.join(stmt)

    def _clone(self, klass=None, **kwargs):
        if klass is None:
            klass = self.__class__
        q = klass(self.domain)
        q.where = self.where._clone()
        q.fields = self.fields[:]
        q.order = self.order
        q.__dict__.update(kwargs)
        return q

    def _get_results(self):
        if self._result_cache is None:
            self._result_cache = self.domain.select(self.to_expression())
        return self._result_cache


class ItemNameQuery(Query):
    def values(self, *fields):
        raise NotImplementedError

    def _get_fields(self):
        # always return itemName() as the sole field
        return ['itemName()']

    def _set_fields(self, value):
        # ignore any attempt to set the fields attribute
        pass

    fields = property(_get_fields, _set_fields)

    def _get_results(self):
        if self._result_cache is None:
            self._result_cache = [item.name for item in 
                                  self.domain.select(self.to_expression())]
        return self._result_cache


class Domain(object):
    def __init__(self, name, simpledb):
        self.name = name
        self.simpledb = simpledb
        self.items = {}

    @property
    def metadata(self):
        return self.simpledb.get_domain_metadata(self)

    def filter(self, *args, **kwargs):
        return self._get_query().filter(*args, **kwargs)

    def select(self, expression):
        return self.simpledb.select(self, expression)

    def all(self):
        return self._get_query()

    def count(self):
        return self._get_query().count()

    def values(self, *args):
        return self._get_query().values(*args)

    def item_names(self):
        return self._get_query().item_names()

    def get(self, name):
        if name not in self.items:
            self.items[name] = Item.load(self.simpledb, self, name)
        item = self.items[name]
        if not item:
            raise ItemDoesNotExist(name)
        return item

    def _encode(self, attribute, value):
        # Encode an attribute, value combination using the simpledb AttributeEncoder.
        return self.simpledb.encoder.encode(self.name, attribute, value)

    def __getitem__(self, name):
        try:
            return self.get(name)
        except ItemDoesNotExist:
            return Item(self.simpledb, self, name, {})

    def __setitem__(self, name, value):
        if not hasattr(value, '__getitem__') or isinstance(value, basestring):
            raise SimpleDBError('Domain items must be dict-like, not `%s`' % type(value))
        del self[name]
        item = Item(self.simpledb, self, name, value)
        item.save()

    def __delitem__(self, name):
        self.simpledb.delete_attributes(self, name)
        if name in self.items:
            del self.items[name]
    
    def __unicode__(self):
        return self.name

    def __iter__(self):
        return iter(self.all())

    def __repr__(self):
        return '<%s: %s>' % ( self.__class__.__name__, unicode(self))

    def _get_query(self):
        return Query(self)


class Item(DictMixin):
    @classmethod
    def load(cls, simpledb, domain, name):
        attrs = simpledb.get_attributes(domain, name)
        return cls(simpledb, domain, name, attrs)

    def __init__(self, simpledb, domain, name, attributes=None):
        self.simpledb = simpledb
        self.domain = domain
        self.name = name
        self.attributes = attributes or {}

    def __getitem__(self, name):
        return self.attributes[name]
    
    def __setitem__(self, name, value):
        self.attributes[name] = value

    def __delitem__(self, name):
        if name in self.attributes:
            self.simpledb.delete_attributes(self.domain, self, {name: self.attributes[name]})
            del self.attributes[name]

    def keys(self):
        return self.attributes.keys()

    def save(self):
        self.simpledb.put_attributes(self.domain, self, self.attributes)
