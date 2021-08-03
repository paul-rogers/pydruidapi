"""
Python wrapper for the Druid REST API. Operations are divided into
those for a host and those for a datasource. Start by configuring
information about the cluster, then issue requests to hosts in
the cluster.

Handles query (GET) requests. At present, does not include
operations to mutate the cluster (POST requests). Contributions
welcome.

This wrapper handles details of mapping operations to URLs, and
parses the JSON results into Python objects.

Typical usage for the default Docker setup:

  import api

  c = api.default_local_docker_config()
  d = Druid(c)
  print(d.leader())
  ds = d.datasource("myds")
  print(ds.segments())

See pydruid (https://github.com/druid-io/pydruid) for a wrapper for the query API

See the API reference (https://druid.apache.org/docs/latest/operations/api-reference.html)
for detais of the API. The API detail docstrings are courtesy of this page.
"""
import requests
import pydruid.client as client

# Common
status_base = "/status"
REQ_STATUS = status_base
REQ_HEALTH = status_base + "/health"
REQ_PROPERTIES = status_base + "properties"
REQ_IN_CLUSTER = status_base + "selfDiscovered/status"

# Coordinator
coord_base = "/druid/coordinator/v1"
REQ_COORD_LEADER = coord_base + "/leader"
REQ_IS_COORD_LEADER = coord_base + "/isLeader"
REQ_LOAD_STATUS = coord_base + "/loadstatus"
REQ_LOAD_QUEUE = coord_base + "/loadqueue"

# Metadata
metadata_base = coord_base + "/metadata"
REQ_MD_DATASOURCES = metadata_base + "/datasources"
REQ_MD_DS_DETAILS = REQ_MD_DATASOURCES + "/{}"
REQ_MD_SEGMENTS = REQ_MD_DS_DETAILS + "/segments"
REQ_MD_SEGMENT_DETAILS = REQ_MD_SEGMENTS + "/{}"

# Datasources
datasource_base = coord_base + "/datasources"
REQ_DATASOURCES = datasource_base
REQ_DS_PROPERTIES = REQ_DATASOURCES + "/{}"
REQ_INTERVALS = REQ_DS_PROPERTIES + "/intervals"
REQ_INTERVAL = REQ_INTERVALS + "/{}"
REQ_INTERVAL_SERVERS = REQ_INTERVAL + "/serverview"
REQ_SEGMENTS = REQ_DS_PROPERTIES + "/segments"
REQ_SEGMENT_DETAILS = REQ_MD_SEGMENTS + "/{}"
REQ_DS_TIERS = REQ_DS_PROPERTIES + "/tiers"

# Retention rules
REQ_RET_RULES = "/druid/coordinator/v1/rules"
REQ_RET_HISTORY = REQ_RET_RULES + "/history"
REQ_DS_RET_RULES = REQ_RET_RULES + "/{}"
REQ_DS_RET_HISTORY = REQ_DS_RET_RULES + "/history"

# Servers
REQ_COORD_SERVERS = coord_base + "/servers"

# Overlord
overlord_base = "/druid/indexer/v1"
REQ_OL_LEADER = overlord_base + "/leader"
REQ_IS_OL_LEADER = overlord_base + "/isLeader"
REQ_TASKS = overlord_base + "/tasks"

# Router
DEFAULT_ROUTER_PORT = 8888
router_base = "/druid/v2"
REQ_ROUTER_DS_LIST = router_base + "/datasources"
REQ_ROUTER_DS = REQ_ROUTER_DS_LIST + "/{}"
REQ_ROUTER_DIMS = REQ_ROUTER_DS + "/dimensions"
REQ_ROUTER_METRICS = REQ_ROUTER_DS + "/metrics"
REQ_ROUTER_SQL = router_base + "/sql"

# Broker
broker_base = "/druid/broker/v1"
REQ_BROKER_STATUS = broker_base + "/loadstatus"
# Note: Broker and Router APIs are identical, but return different data
# depending on the service (port) to which they are sent.
REQ_BROKER_DS_LIST = router_base + "/datasources"
REQ_BROKER_DS = REQ_BROKER_DS_LIST + "/{}"
REQ_BROKER_DIMS = REQ_BROKER_DS + "/dimensions"
REQ_BROKER_METRICS = REQ_BROKER_DS + "/metrics"

# Lookups
lookup_base = "/druid/coordinator/v1/lookups/config"
REQ_LU_CONFIG = lookup_base
REQ_LU_ALL_LOOKUPS = REQ_LU_CONFIG + "/all"
REQ_LU_TIER_CONFIG = lookup_base + "/{}"
REQ_LU_LOOKUP_CONFIG = REQ_LU_TIER_CONFIG + "/{}"

DEFAULT_TIER = "_default_tier"

# Enable this option to see the URLs as they are sent.
# Handy for debugging.
trace = False

class NodeConfig:
    """
    NodeConfig describes one node in a Druid cluster. A node is a
    "role" listening on port at some IP address (or host name). There
    can be multiple nodes per host and per Java process on that host.
    If a single host/port combination assumes multiple roles, then
    the combination will appear multiple times, each with a distinct
    role.
    """
    
    ROUTER = 'router'
    BROKER = 'broker'
    COORDINATOR = 'coordinator'
    OVERLORD = 'overlord'
    HISTORICAL ='historical'
    INDEXER = 'indexer'
    MIDDLE_MANAGER = 'middle_manager'
    PEON = 'peon'
    ROLES = [ROUTER, BROKER, COORDINATOR, OVERLORD, HISTORICAL, INDEXER, MIDDLE_MANAGER, PEON]

    def __init__(self, host, port, role, protocol="http:"):
        self.host_name = host
        self.port = port
        self.endpoint = endpoint(host, port)
        self.roles = [role]
        self.protocol = protocol
        self.private_host = host
        self.private_port = port
        self.private_endpoint = self.endpoint
        self.tier = None
        self.prefix = None
        
    def is_a(self, role):
        return role in self.roles

    def url_prefix(self):
        if self.prefix is None:
            self.prefix = self.protocol + "//" + self.endpoint
        return self.prefix

def endpoint(host, port):
    return host + ":" + str(port)

def node_from_metadata(json):
    tls_port = json['tls_port']
    if tls_port == -1:
        protocol = "http:"
        port = json['plaintext_port']
    else:
        protocol = "https:"
        port = tls_port
    node = NodeConfig(json['host'], port, json['server_type'], protocol)
    node.tier = json['tier']
    return node

def router_node( host="localhost", port=DEFAULT_ROUTER_PORT, role=NodeConfig.ROUTER, protocol="http:"):
    return NodeConfig(host, port, role, protocol)

def role_name(role) -> str:
    return role

class Config:
    """
    Config describes a Druid cluster in enough detail to run
    this API. We need the protocol (http or https), the set
    of nodes and their roles.
    """
    
    def __init__(self, router):
        self.router = router
        self.docker_host = None
        self.port_map = {}

    def map_node(self, host, port):
        if self.docker_host is None:
            return host, port
        try:
            mapped_port = self.port_map[port]
            return self.docker_host, mapped_port
        except KeyError:
           return host, port

def default_docker_config(host_name, router_port=DEFAULT_ROUTER_PORT):
    """
    Creates a Druid cluster configuration for the "out-of-the-box"
    default Docker configuration as used for a single-machine
    trial setup.

    host_name : str
        Name of the host on which Docker runs.
    router_port : int, default=8888
        Port for the router API (and Console UI). The default value
        conflicts with Jupyter, so may be changed.
    """
    router = router_node(host_name, router_port)
    c = Config(router)
    c.docker_host = host_name
    if router_port != DEFAULT_ROUTER_PORT:
        c.port_map[DEFAULT_ROUTER_PORT] = router_port
    return c

def default_local_docker_config(router_port=8888):
    """
    Creates a Druid cluster configuration for the "out-of-the-box"
    default Docker configuration as used for a single-machine
    trial setup on the local machine.
    """
    return default_docker_config("localhost", router_port)

class Error(Exception):
    
    pass

class InvalidHostError(Error):
    
    def __init__(self, host_name):
        self.message = "Invalid host: " + host_name
    
class RestError(Error):
    
    def __init__(self, msg, url, status):
        self.message = msg
        self.url = url
        self.status = status

class DruidError(Error):
    
    def __init__(self, msg):
        self.message = msg

class NotFoundError(Error):
    """
    Raised for 404 response errors when the API is reasonably sure that the
    error indicates an missing resource.
    """
    
    def __init__(self, msg):
        self.message = msg


def encode_interval(interval_id):
    return interval_id.replace("/", "_")
class Service:

    def __init__(self, node):
        self.node = node
    
    #-------- Common --------
    
    def status(self):
        r = self.node.get(self.node.url(REQ_STATUS))
        return r.json()
    
    def is_healthy(self):
        try:
            r = self.node.get(self.node.url(REQ_HEALTH))
            result = r.json()
            return result == True
        except RestError:
            return False
    
    def properties(self):
        r = self.node.get(self.node.url(REQ_PROPERTIES))
        return r.json()
    
    def in_cluster(self):
        r = self.node.get(self.node.url(REQ_IN_CLUSTER))
        return r.json()["selfDiscovered"]
    
class Coordinator(Service):
    """
    Coordinator is a Druid service with the coordinator role.


    The coordinator provides overlapping "metadata" and "datasource" requests.
    Here, methods with "md" in the name are for metadata.
    """

    def __init__(self, node):
        Service.__init__(self, node)

    def lead(self) -> str:
        """
        Returns the current leader Coordinator of the cluster.
        """
        r = self.node.get(self.node.url(REQ_COORD_LEADER))
        return r.json()
    
    def is_lead(self) -> bool:
        """
        Returns True if this server is the current leader Coordinator of the cluster,
        False otherwise.
        """
        r = self.node.get(self.node.url(REQ_IS_COORD_LEADER))
        return r.json()["leader"]
    
    def load_status_percent(self):
        """
        Returns the percentage of segments actually loaded in the cluster versus 
        segments that should be loaded in the cluster.
        """
        r = self.node.get(self.node.url(REQ_LOAD_STATUS))
        return r.json()
    
    def load_status_count(self):
        """
        Returns the number of segments left to load until segments that should 
        be loaded in the cluster are available for queries. This does not include 
        segment replication counts.
        """
        r = self.node.get(self.node.url(REQ_LOAD_STATUS), {"simple": ""})
        return r.json()
    
    def load_status_full(self):
        """
        Returns the number of segments left to load in each tier until segments 
        that should be loaded in the cluster are all available. This includes 
        segment replication counts.
        """
        r = self.node.get(self.node.url(REQ_LOAD_STATUS), {"full": ""})
        return r.json()
    
    def load_queue_segs(self):
        """
        Returns the ids of segments to load and drop for each Historical process.
        """
        r = self.node.get(self.node.url(REQ_LOAD_QUEUE))
        return r.json()
    
    def load_queue_simple(self):
        """
        Returns the number of segments to load and drop, as well as the total 
        segment load and drop size in bytes for each Historical process.
        """
        r = self.node.get(self.node.url(REQ_LOAD_QUEUE), {"simple": ""})
        return r.json()
    
    def load_queue_full(self):
        """
        Returns the segments to load and drop for each Historical process.
        """
        r = self.node.get(self.node.url(REQ_LOAD_QUEUE), {"full": ""})
        return r.json()
    
    #-------- Coordinator Metadata --------
    
    def md_ds_names(self, include_unused=False):
        """
        Returns a list of the names of data sources in the metadata store.
        
        include_unused : bool, default is False
            if False, returns only datasources with at least one used segment
            in the cluster.
        """
        params = None
        if include_unused:
            params = {"includeUnused": ""}
        r = self.node.get(self.node.url(REQ_MD_DATASOURCES), params)
        return r.json()
    
    def all_md_ds_details(self):
        """
        Returns a list of all data sources with at least one used segment in 
        the cluster. Returns all metadata about those data sources as stored 
        in the metadata store.
        """
        r = self.node.get(self.node.url(REQ_MD_DATASOURCES), {"full": ""})
        return r.json()

    def md_ds_details(self, ds_name):
        """
        Returns full metadata for a datasource as stored in the metadata store.
        
        ds_name: str
            name of the datasource to query
        """
        r = self.node.get(self.node.url(REQ_MD_DS_DETAILS, [ds_name]))
        return r.json()

    def md_segments(self, ds_name, full=False):
        """
        Returns a list of all segments for a datasource as stored in the metadata store.
        
        ds_name: str
            name of the datasource to query
        full: bool, default is False
            includes the full segment metadata as stored in the metadata store.
        """
        params = None
        if full:
            params = {"full": ""}
        r = self.node.get(self.node.url(REQ_MD_SEGMENTS, [ds_name]))
        return r.json()

    def segment_metadata(self, ds_name, segment_id):
        """
        Returns full segment metadata for a specific segment as stored in the metadata store.
       
        ds_name: str
            name of the datasource to query
        segment_id: str
            id of the segment as returned from md_segments(). This method handles
            encoding into the form required by the API.
        """
        r = self.node.get(self.node.url(REQ_MD_SEGMENT_DETAILS, [ds_name, segment_id]))
        return r.json()

    #-------- Coordinator Datasources --------
    
    def cluster_datasources(self):
        """
        Returns a list of datasource names found in the cluster.
        """
        r = self.node.get(self.node.url(REQ_DATASOURCES))
        return r.json()  
    
    def all_cluster_ds_properties(self):
        """
        Returns a list of objects containing the name and properties of datasources 
        found in the cluster. Properties include segment count, total segment byte 
        size, replicated total segment byte size, minTime, and maxTime.
        """
        r = self.node.get(self.node.url(REQ_DATASOURCES), {"simple": ""})
        return r.json()

    def all_cluster_ds_metadata(self):
        """
        Returns a list of datasource names found in the cluster with all 
        metadata about those datasources.
        """
        r = self.node.get(self.node.url(REQ_DATASOURCES), {"full": ""})
        return r.json()
    
    def cluster_ds_properties(self, ds_name):
        """
        Returns an object containing the name and properties of a datasource. 
        Properties include segment count, total segment byte size, replicated 
        total segment byte size, minTime, and maxTime.
       
        ds_name: str
            name of the datasource to query

        See DataSource.properties()
        """
        r = self.node.get(self.node.url(REQ_DS_PROPERTIES, [ds_name]))
        return r.json()
    
    def cluster_ds_metadata(self, ds_name):
        """
        Returns full metadata for a datasource.
       
        ds_name: str
            name of the datasource to query

        See DataSource.metadata()
        """
        r = self.node.get(self.node.url(REQ_DATASOURCES, [ds_name]), {"full": ""})
        return r.json()
    
    def intervals(self, ds_name, params=None):
        """
        Internal method which information about intervals for a datasource.

        ds_name: str
            name of the datasource to query
        params: dict, default is None
            None: Returns a set of segment intervals. Each is a string in the format
                2012-01-01T00:00:00.000/2012-01-03T00:00:00.000.
            {"simple": ""}: Returns a map of an interval to n object containing the total 
                byte size of segments and number of segments for that interval.
            {"full":, ""}: Returns a map of an interval to a map of segment metadata to a 
                 set of server names that contain the segment for that interval.

        See DataSource.intervals(), all_interval_segments(), all_interval_details()
        """
        r = self.node.get(self.node.url(REQ_INTERVALS, [ds_name]), params)
        return r.json()

    def interval(self, ds_name, interval_id, params=None):
        """
        Internal method which information about one interval for a datasource.

        ds_name: str
            name of the datasource to query
       interval_id : str
            interval ID as returned from intervals()
        params: dict, default is None
            None: Returns a set of segment ids for an interval.
            {"simple": ""}: Returns a map of segment intervals contained within the specified interval to 
                an object containing the total byte size of segments and number of segments 
                for an interval.
            {"full":, ""}: Returns a map of segment intervals contained within the specified interval 
                to a map of segment metadata to a set of server names that contain the 
                segment for an interval.

        See DataSource.interval(), interval_segments(), interval_mapping()
        """
        enc = encode_interval(interval_id)
        r = self.node.get(self.node.url(REQ_INTERVAL, [ds_name, enc]), params)
        return r.json()

    def interval_servers(self, ds_name, interval_id):
        """
        Returns a map of segment intervals contained within the specified interval to information
        about the servers that contain the segment for an interval.
       
        ds_name: str
            name of the datasource to query
        interval_id : str
            interval ID as returned from intervals()

        See DataSource.interval_servers()
        """
        enc = encode_interval(interval_id)
        r = self.node.get(self.node.url(REQ_INTERVAL_SERVERS, [ds_name, enc]))
        return r.json()

    def ds_tiers(self, ds_name):
        """
        Return the tiers that a datasource exists in.
       
        ds_name: str
            name of the datasource to query

        See DataSource.tiers()
        """
        r = self.node.get(self.node.url(REQ_DS_TIERS, [ds_name]))
        return r.json()

    #-------- Coordinator Retention Rules --------

    def all_retention_rules(self):
        """
        Returns all rules asobjects for all datasources in the cluster 
        including the default datasource.
        """
        r = self.node.get(self.node.url(REQ_RET_RULES))
        return r.json()
    
    def all_audit_history(self, count=25):
        """
        Returns last entries of audit history of rules for all datasources.

        count : int, default = 25
            Number of audit entries to return
        """
        r = self.node.get(self.node.url(REQ_RET_HISTORY), {"count": str(count)})
        return r.json()
    
    def ds_retention_rules(self, ds_name):
        """
        Returns retention all rules for a specified datasource.

        ds_name: str
            name of the datasource to query

        See DataSource.retention_rules()
        """
        r = self.node.get(self.node.url(REQ_DS_RET_RULES, [ds_name]))
        return r.json()

    def ds_audit_history(self, ds_name, count=25):
        """
        Returns last entries of audit history of rules for a specified datasource.

        ds_name: str
            name of the datasource to query
        count : int, default = 25
            Number of audit entries to return

        See DataSource.audit_history()
        """
        r = self.node.get(self.node.url(REQ_RET_HISTORY), {"count": str(count)})
        return r.json()

    def servers(self):
        """
        Returns a list of servers URLs using the format {hostname}:{port}. Note that 
        processes that run with different types will appear multiple times with 
        different ports.
        """
        r = self.node.get(self.node.url(REQ_COORD_SERVERS))
        return r.json()

    def server_details(self):
        """
        Returns a list of server data objects in which each object has the following keys:

        host: host URL include ({hostname}:{port})
        type: process type (indexer-executor, historical)
        currSize: storage size currently used
        maxSize: maximum storage size
        priority
        tier
        """
        r = self.node.get(self.node.url(REQ_COORD_SERVERS), {"simple": ""})
        return r.json()
    

class Router(Service):

    def __init__(self, node):
        Service.__init__(self, node)

    def datasources(self):
        """
        Returns a list of queryable datasources.
        """
        r = self.node.get(self.node.url(REQ_ROUTER_DS_LIST))
        return r.json()
    
    def schema(self, ds_name):
        """
        Returns the dimensions and metrics of the datasource.

        ds_name: str
            name of the datasource to query
 
        See DataSource.schema()
        """
        r = self.node.get(self.node.url(REQ_ROUTER_DS, [ds_name]))
        return r.json()
    
    def dimensions(self, ds_name):
        """
        Returns the dimensions of the datasource.

        ds_name: str
            name of the datasource to query
 
        See DataSource.dimensions()
        """
        r = self.node.get(self.node.url(REQ_ROUTER_DIMS, [ds_name]))
        return r.json()
    
    def metrics(self, ds_name):
        """
        Returns the metrics of the datasource.

        ds_name: str
            name of the datasource to query
 
        See DataSource.metrics()
        """
        r = self.node.get(self.node.url(REQ_ROUTER_METRICS, [ds_name]))
        return r.json()

    def sql(self, query):
        query_obj = {"query": query}
        r = self.node.post_json(self.node.url(REQ_ROUTER_SQL), query_obj)
        return r.json()

    def servers(self):
        sql = '''
          SELECT\n
            "server", "server_type", "tier", "host", "plaintext_port",
            "tls_port", "curr_size", "max_size"
          FROM sys.servers   
        '''
        result = self.sql(sql)
        return result

class Broker(Service):

    def __init__(self, node):
        Service.__init__(self, node)

    def is_ready(self):
        """
        Returns True if the Broker knows about all segments in the cluster. 
        This can be used to learn when a Broker process is ready to be 
        queried after a restart.
        """
        r = self.node.get(self.node.url(REQ_BROKER_STATUS))
        return r.json()['inventoryInitialized'] == True

    def datasources(self):
        """
        Returns a list of queryable datasources.
        """
        r = self.node.get(self.node.url(REQ_BROKER_DS_LIST))
        return r.json()
    
    def schema(self, ds_name):
        """
        Returns the dimensions and metrics of the datasource.

        ds_name: str
            name of the datasource to query
 
        See DataSource.schema()
        """
        r = self.node.get(self.node.url(REQ_BROKER_DS, [ds_name]))
        return r.json()

    def full_schema(self, ds_name):
        """
        Returns the list of served intervals with dimensions and metrics
        being served for those intervals.

        ds_name: str
            name of the datasource to query
 
        See DataSource.full_schema()
        """
        r = self.node.get(self.node.url(REQ_BROKER_DS, [ds_name]), {"full": ""})
        return r.json()
    
    def dimensions(self, ds_name):
        """
        Returns the dimensions of the datasource.

        ds_name: str
            name of the datasource to query
 
        See DataSource.dimensions()
        """
        r = self.node.get(self.node.url(REQ_BROKER_DIMS, [ds_name]))
        return r.json()
    
    def metrics(self, ds_name):
        """
        Returns the metrics of the datasource.

        ds_name: str
            name of the datasource to query
 
        See DataSource.metrics()
        """
        r = self.node.get(self.node.url(REQ_BROKER_METRICS, [ds_name]))
        return r.json()

def map_lookup(vers, lookups, injective=False):
    """
    Creates a Map lookup request.

    vers: str
        An arbitrary string assigned by the user. When making updates to an
        existing lookup the value must be lexicographically greater than
        the existing version.
    lookups: dict
        A dictionary of key/value pairs, both of which must be strings.
    injective: bool, default = False
        Marks the lookup as injective, per this reference:
        https://druid.apache.org/docs/latest/querying/lookups.html#query-execution
    """
    return {
        "version": vers,
        "lookupExtractorFactory": {
            "type": "map",
            "injective": injective,
            "map": lookups
            }    
        }

class Lookups(Service):
    """
    Lookups is a "virtual" service that provides Druid Lookup operations.
    The underlying service is a coordinator.

    See https://druid.apache.org/docs/latest/querying/lookups.html
    """

    def __init__(self, node):
        Service.__init__(self, node)

    def initialize(self):
        """
        If you have NEVER configured lookups before, you MUST call this method
        to initialize the configuration.
        """
        obj = {
            DEFAULT_TIER: {}
        }
        self.node.post(self.node.url(REQ_LU_CONFIG), obj)

    def tiers(self):
        """
        Returns a list of known tier names. Returns an empty
        list if lookups are not get initialized.

        See initialize()
        """
        try:
            r = self.node.get(self.node.url(REQ_LU_CONFIG))
            return r.json()
        except RestError as e:
            return []

    def all_lookups(self):
        """
        Returns all known lookup specs for all tiers ibulk update format,
        which is a dictionary of tiers which contain a dictionary of lookups.
        Returns an empty dictionary if lookups are not get initialized.

        See initialize()

        See the bulk update format at
        https://druid.apache.org/docs/latest/querying/lookups.html#bulk-update
        """
        try:
            r = self.node.get(self.node.url(REQ_LU_ALL_LOOKUPS))
            return r.json()
        except RestError as e:
            return {}

    def lookups_for_tier(self, tier=DEFAULT_TIER):
        """
        Returns a list of lookup names for the given tier.

        tier: str, default = DEFAULT_TIER
            Name of the tier to query.

        Raises NotFoundError if the tier is undefined or lookups
        are not initialized.
        """
        try:
            r = self.node.get(self.node.url(REQ_LU_TIER_CONFIG, [tier]))
            return r.json()
        except RestError as e:
            raise NotFoundError("tier = " + tier)
        
    def lookup(self, tier, id):
        """
        Returns the lookup spec for the given id and tier.

        tier: str
            Name of the tier to query.
        id: str
            id of the lookup assigned at creation or returned from
            lookups_for_tier()

        Raises NotFoundError if the tier is undefined, the lookup id
        is undefined or if lookups are not initialized.
        """
        try:
            r = self.node.get(self.node.url(REQ_LU_LOOKUP_CONFIG, [tier, id]))
            return r.json()
        except RestError as e:
            raise NotFoundError("tier = {}, lookup id = {}".format(tier, id))
    
    def update(self, tier, id, defn):
        """
        Create or replace a lookup in the given tier. If updating, the
        version number of the new lookup must be greater than that of
        the existign lookup.

        tier: str
            Name of the tier to update.
        id: str
            id of the lookup to create or update
        defn: obj | dict
            Definition object as a Python object which is converted to
            JSON internally.
        """
        self.node.post_json(self.node.url(REQ_LU_TIER_CONFIG, [tier]), defn)

class Node:
    """
    Node is a wrapper around a Druid process running on a host
    at a given port. A Druid service can run one or more roles
    (broker, coordinator, etc.) Each role is modeled as a distinct
    Node. Node provides wrappers for each Druid API, handling
    the details of  URL creation, JSON parsing, etc.

    The host configuration provides the machine name and port.

    This base class provides functions common to all Druid roles.
    """
    
    def __init__(self, druid, config):
        self.druid = druid
        self.config = config
        self.session = requests.Session()

    def close(self):
        self.session.close()
    
    def url(self, req, args=None) -> str:
        """
        url returns a URL path, excluding query parameters,
        for the present node, using the URL provided.
        
        req : str
            relative URL, with optional {} placeholders

        args : list
            optional list of values to match {} placeholders
            in the URL.
        """
        url = self.config.url_prefix() + req
        if args is not None:
            url = url.format(*args)
        return url
    
    def get(self, url, params=None) -> requests.Request:
        """
        get issues a GET request for the given URL on this node,
        with the optional URL query parameters provided.
        
        url : str
            Full url, without a query string

        params : dict
            Optional dictionary of query parameters.
        """
        r = self.session.get(url, params=params)
        if trace:
            print(r.url)
        if r.status_code != requests.codes.ok:
            raise RestError("Request failed", r.url, r.status_code)
        return r

    def post(self, url, body, params=None) -> requests.Request:
        """
        post issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters. The payload is serialized to JSON.
        """
        if trace:
            print("body:", body)
        r = self.session.post(url, data=body) # , params=params)
        if trace:
            print(r.url)
        if r.status_code != requests.codes.ok and r.status_code != requests.codes.accepted:
            print(r.text)
            raise RestError("Request failed", r.url, r.status_code)
        return r

    def post_json(self, url, body, params=None) -> requests.Request:
        """
        post issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters. The payload is serialized to JSON.
        """
        if trace:
            print("body:", body)
        r = self.session.post(url, json=body) # , params=params)
        if trace:
            print(r.url)
        if r.status_code != requests.codes.ok and r.status_code != requests.codes.accepted:
            if trace:
                print(r.text)
            raise RestError("Request failed", r.url, r.status_code)
        return r

    def as_common(self) -> Service:
        return Service(self)
    
    def as_coordinator(self) -> Coordinator:
        return Coordinator(self)

    def as_router(self) -> Router:
        return Router(self)

    def as_broker(self) -> Broker:
        return Broker(self)

    def as_service(self, role) -> Service:
        if role == NodeConfig.ROUTER:
            return self.as_router()
        if role == NodeConfig.BROKER:
            return self.as_broker()
        if role == NodeConfig.COORDINATOR:
            return self.as_coordinator()
        #if role == NodeConfig.HISTORICAL:
        #    return "historical"
        #if role == NodeConfig.MIDDLE_MANAGER:
        #    return "middle manager"
        return self.as_common()

class DataSource:
    """
    The DataSource class represents one Druid datasource. The class holds the
    datasource name and will infer the coordinator to which to send requests.
    """
    
    def __init__(self, druid, name):
        self.druid = druid
        self.name = name
    
    def coord(self):
        return self.druid.lead_coordinator()

    def router(self):
        return self.druid.router()
 
    def broker(self):
        return self.druid.broker()

    def schema_node(self):
        """
        Returns the node to query for schema. This should be the router, if
        available. But, at the time of this writing, the router returns
        nothing. So, always uses the broker.
        """
        #node = self.router()
        #if node is not None:
        #    return node
        return self.broker()
   
    def metadata(self):
        return self.coord().md_ds_details(self.name)
    
    def properties(self):
        return self.coord().cluster_ds_properties(self.name)
    
    def cluster_metadata(self):
        return self.coord().cluster_ds_metadata(self.name)
    
    def segment_ids(self):
        return self.coord().md_segments(self.name)
    
    def segment_details(self):
        return self.coord().md_segments(self.name, True)
        
    def segment(self, segment_id):
        return self.coord().segment_metadata(self.name, segment_id)

    def intervals(self):
        """
        Returns a set of segment intervals. Each is a string in the format
        2012-01-01T00:00:00.000/2012-01-03T00:00:00.000."""
        return self.coord().intervals(self.name)

    def all_interval_segments(self):
        """
        Returns a map of an interval to n object containing the total byte 
        size of segments and number of segments for that interval.
        """
        return self.coord().intervals(self.name, {"simple": ""})

    def all_interval_details(self):
        """
        Returns a map of an interval to a map of segment metadata to a 
        set of server names that contain the segment for that interval.
        """
        return self.coord().intervals(self.name, {"full": ""})

    def interval(self, interval_id):
        """
        Returns a set of segment ids for an interval.
        """
        return self.coord().interval(self.name, interval_id)

    def interval_segments(self, interval_id):
        """
        Returns a map of segment intervals contained within the specified interval to 
        an object containing the total byte size of segments and number of segments 
        for an interval.
        """
        return self.coord().interval(self.name, interval_id, {"simple": ""})

    def interval_mapping(self, interval_id):
        """
        Returns a map of segment intervals contained within the specified interval 
        to a map of segment metadata to a set of server names that contain the 
        segment for an interval.

        interval_id : str
            interval ID as returned from intervals()
        """
        return self.coord().interval(self.name, interval_id, {"full": ""})

    def interval_servers(self, interval_id):
        """
        Returns a map of segment intervals contained within the specified interval to information
        about the servers that contain the segment for an interval.

        interval_id : str
            interval ID as returned from intervals()
        """
        return self.coord().interval_servers(self.name, interval_id)

    def tiers(self):
        """
        Return the tiers that this datasource exists in.
         """
        return self.coord().ds_tiers(self.name)

    def retention_rules(self):
        """
        Returns retention all rules for this datasource.
        """
        return self.coord().ds_retention_rules(self.name)

    def audit_history(self, count=25):
        """
        Returns last entries of audit history of rules for this datasource.

        count : int, default = 25
            Number of audit entries to return
        """
        return self.coord().ds_audit_history(self.name, count)

    def schema(self):
        """
        Returns the dimensions and metrics of the datasource.
        """
        return self.schema_node().schema(self.name)
    
    def full_schema(self):
        """
        Returns the list of served intervals with dimensions and metrics
        being served for those intervals.
        """
        # Only available from the broker, not the router.
        return self.broker().full_schema(self.name)
    
    def dimensions(self):
        """
        Returns the dimensions of the datasource.
        """
        return self.schema_node().dimensions(self.name)
    
    def metrics(self):
        """
        Returns the metrics of the datasource.
        """
        return self.schema_node().metrics(self.name)

    def analyze(self):
        result = self.druid.query_client().segment_metadata(
            datasource=self.name, 
            merge=True,
            analysisTypes=["cardinality", "interval", "minmax", "size"])
        return result.result[0]

class NodeStats:

    def __init__(self, json):
        self.endpoint = json['server']
        self.role = json['server_type']
        self.tier = json['tier']
        self.private_host = json['host']
        self.host_name = self.private_host
        tls_port = json['tls_port']
        if tls_port == -1:
            self.protocol = "http"
            self.port = json['plaintext_port']
        else:
            self.protocol = "https"
            self.port = tls_port
        self.private_port = self.port

class Druid:
    """
    The Druid class represents the overall Druid cluster. It holds the
    list of nodes (servers, hosts) which make up the cluster. Each
    host is "virtual": multiple reside on the same machine, for example,
    when running in Docker. Each must have a unique (host name, port)
    pair. An optional alias allows labeling nodes by function and is
    required if two nodes share the same host name.
    """
    
    def __init__(self, config):
        self.config = config
        self.node_configs = {}
        self.nodes = {}
        self.private_nodes = {}
        self._lead_coordinator = None
        self._datasources = {}
        self._router = None
        self._broker = None
        self._query_client = None
        self.boot()
    
    def boot(self):
        router_config = self.config.router
        router_node = Node(self, self.config.router)
        router = Router(router_node)
        servers = router.servers()
        #print(vars(router_config))
        for server in servers:
            #print(server)
            node_config = node_from_metadata(server)
            host, port = self.config.map_node(node_config.host_name, node_config.port)
            if host != node_config.host_name or port != node_config.port:
                node_config.host_name = host
                node_config.port = port
                node_config.endpoint = endpoint(host, port)
            try:
                existing = self.node_configs[node_config.endpoint]
                existing.roles.append(node_config.roles[0])
                continue
            except KeyError:
                pass
            if router_config.endpoint == node_config.endpoint:
                # Preserve the router node and its config
                router_config.private_host = node_config.private_host
                router_config.private_port = node_config.private_port
                router_config.private_endpoint = node_config.private_endpoint
                node_config = router_config
                self._router = router
                self.nodes[router_config.endpoint] = router_node
                router_node = None
                #print("router")
            self.node_configs[node_config.endpoint] = node_config
            self.private_nodes[node_config.private_endpoint] = node_config.endpoint
            #print(vars(node_config))
        if router_node is not None:
            router_node.close()

    def node(self, endpoint) -> Node:
        try:
            endpoint = self.private_nodes[endpoint]
        except KeyError:
            pass
        try:
            return self.nodes[endpoint]
        except KeyError:
            pass
        try:
            config = self.node_configs[endpoint]
            host = Node(self, config)
            self.nodes[endpoint] = host
            return host
        except KeyError:
            raise InvalidHostError(endpoint) from None
    
    def config_for_service(self, service) -> Service:
        """
        Returns the first node listed in the node config list
        for the given service.
        """
        for h in self.node_configs.values():
            if h.is_a(service):
                return h
        return None  
  
    def service(self, service) -> Service:
        h = self.config_for_service(service)
        if h is None:
            return None
        node = self.node(h.endpoint)
        if node is None:
            return None
        return node.as_service(service)

    def lead_coordinator(self) -> Coordinator:
        # Return lead coordinator if known (may be out of date)
        if self._lead_coordinator is not None:
            return self._lead_coordinator
        # Look for a node labeled as coordinator
        node = self.service(NodeConfig.COORDINATOR)
        if node is None:
            raise DruidError("No coordinator identified")
            
        # Is that coordinator the leader?
        if node.is_lead():
            self._lead_coordinator = node
            return self._lead_coordinator
        
        # Ask the leader name and try again. Note: this will fail
        # in Docker, AWS as we are not given the external host name.
        leader_name = node.lead()
        node = self.host(leader_name)
        self._lead_coordinator = node
        return self._lead_coordinator

    def lookups(self) -> Lookups:
        coord = self.lead_coordinator()
        return Lookups(coord.node)
    
    def router(self) -> Router:
        if self._router is None:
            self._router = self.service(NodeConfig.ROUTER)
        return self._router

    def broker(self) -> Broker:
        if self._broker is None:
            self._broker = self.service(NodeConfig.BROKER)
        return self._broker

    def datasource(self, name) -> DataSource:
        try:
            return self._datasources[name]
        except KeyError:
            pass
        ds = DataSource(self, name)
        self._datasources[name] = ds
        return ds

    def servers(self):
        return self.router().servers()
    
    def datasources(self):
        names = self.lead_coordinator().md_ds_names()
        dsmap = {}
        for n in names:
            dsmap[n] = self.datasource(n)
        return dsmap

    def query_client(self):
        if self._query_client is None:
            coord = self.router()
            self._query_client = client.PyDruid(coord.node.config.url_prefix(), 'druid/v2')
        return self._query_client
