from contextlib import contextmanager
from datetime import datetime
from functools import wraps, partial
from memojito import mproperty
#from monkeylib.exc import HTTPException, HTTPNotFound
from webob.exc import HTTPError, HTTPNotFound
from webob.dec import wsgify
import hashlib
import logging
import os
import sys
import time


def nuuid(*args):
    uobj = hashlib.md5()
    [uobj.update(str(x)) for x in args]
    return uobj.hexdigest()[:6]

_marker = object()

        
class TraceProfile(object):
    """
    Context manager and decorator for identifying, timing and logging
    times for a block of code.

    Meant for timing network interactions.
    """ 

    HEADER = 'X-Request-Trace'
    IGNORE_HEADER = 'X-Ignore-Request-Trace'
    httperror, httpnotfound = HTTPError, HTTPNotFound
    
    
    def __init__(self, msg="%(uid)s %(trace_time)s %(parents)s",
                 logname='monkeylib.instr', uid=_marker, parents=None,
                 quiet=False, level=logging.DEBUG, **kwargs):
        """
        msg

           a formatting string for outputting the trace information
           
        logname

           which logger to use, usually instr.request or instr.response
        
        uid
        
           identifier for this trace. If left as _marker, uid will be
           generated. Set to None if trace uids are not needed.

        parents

           : delimited string of identifiers for upstream events

        kwargs

           anything else required by the 'msg' format string
        """
        self.level = level
        self.msg = msg
        self.pid = os.getpid()
        self.extra = kwargs
        self.uid = uid
        self.logname = logname
        self.created = timestamp()
        self.origin, self.grandpa, self.parent = ('', '', '')
        self.lineage = self.unpack_lineage(parents)
        self.elapsed = None

    def unpack_lineage(self, parents):
        """
        Unpack linear relationship into origin, grandparent and parent
        """
        null = '...'.split('.')
        if parents is None:
            parents = null

        parents = [x for x in parents if x]
        if not parents:
            return null
        
        plen = len(parents)
        
        if plen == 3:
            # has origin, grandpa and parent
            self.origin, self.grandpa, self.parent = parents
        elif plen == 2:
            # origin is grandpa
            self.origin, self.parent = parents
            self.grandpa = self.origin
        else:
            # parent is origin
            val = parents[0]
            self.origin, self.grandpa, self.parent = [val for x in xrange(3)]
        return self.origin, self.grandpa, self.parent
    
    uuid_gen = staticmethod(nuuid)
    
    @mproperty
    def log(self):
        if self.logname:
            logger = logging.getLogger(self.logname)
            logger = logging.LoggerAdapter(logger, self.extra)
            return partial(logger.log, self.level)
        return None

    def __enter__(self):
        if self.uid is not None and self.uid is _marker:
            self.uid = self.uuid_gen(":".join(self.lineage), self.created, str(self.pid))
        self.clock_start_time = time.clock()
        self.wall_start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.extra['status'] = self.extra.get('status', 0)
        if exc_type is self.httpnotfound:
            self.extra['status'] = 404
        elif exc_type and issubclass(exc_type, self.httperror):
            self.extra['status'] = 500
        elif exc_type and issubclass(exc_type, Exception):
            self.extra['status'] = 600            

        clock_end = time.clock()
        wall_end = time.time()
        
        self.extra['uid'] = self.uid
        self.extra['parents'] = "%s:%s" %(self.origin, self.parent)
        self.extra['clock_time'] = self.clock_elapsed = clock_end - self.clock_start_time
        self.extra['wall_time'] = self.wall_elapsed = wall_end - self.wall_start_time
        if self.log is not None:
            self.log(self.msg, self.extra)
        return False

    def make_header(self, parent_headers=None, base_trace = "::"):
        trace = base_trace
        if parent_headers is not None:
            trace = parent_headers.get(self.HEADER)
            if trace is None:
                raise ValueError("No %s header found" %self.HEADER)
            origin, parent = self.parse_header(trace, parent_only=True)
        else:
            origin, parent = self.origin and self.origin or '',\
                             self.parent and self.parent or '',
        return ":".join((origin, parent, self.uid))

    @classmethod
    def parents(cls, headers):
        header = headers.get(cls.HEADER)
        if header is not None:
            return cls.parse_header(header)
        return '','',''

    @staticmethod
    def parse_header(header, parent_only=False):
        origin, grandparent, parent = header.split(":")
        if not parent_only:
            return origin, grandparent, parent
        return origin, parent

    @classmethod
    def decorate(cls, prof):
        def wrap_func(func):
            @wraps(func)
            def wrapper(*args, **kw):
                with prof:
                    return func(*args, **kw)
            return wrapper
        return wrap_func

    @classmethod
    def from_headers(cls, headers, msg="%(parents)s:%(uid)s %(trace_time)s",
                     logname='instr.request', uid=_marker, **kwargs):
        parents = cls.parents(headers)
        return cls(msg, logname, uid, parents, **kwargs)

    lognames = 'instr', 'instr.inbound', 'instr.inbound',

    default_formatter = logging.Formatter("%(levelname)-10s %(asctime)s %(message)s")
    default_handler = logging.StreamHandler(sys.stderr)

    def prepare_outbound(self, headers, **kw):
        self.extra.update(kw)
        headers = headers.copy()
        headers[self.HEADER] = self.make_header()
        if self.logname is None:
            headers[self.IGNORE_HEADER] = 'true'
        return headers

    @classmethod
    def configure_logs(cls, handler=default_handler, formatter=default_formatter):
        for name in cls.lognames:
            logger = logging.getLogger(name)
            handler.setFormatter(formatter)
            logger.setHandler(handler)


trace_blacklist = ['/']
trace_prefix_blacklist = ['_status', 'hax', 'pid', '_baboon', 'js', 'css', 'img']


@wsgify.middleware
def logging_timer_mw(req, app, logname='instr.inbound',\
                     exclude=frozenset(trace_blacklist),
                     exclude_prefix=frozenset(trace_prefix_blacklist)):
    exclude = req.path_info_peek() in exclude_prefix or req.path in exclude
    if exclude or req.headers.get(TraceProfile.IGNORE_HEADER):
        logname = None
    with TraceProfile.from_headers(req.headers,
                                   msg="%(parents)s:%(uid)s %(wall_time)f %(path)s %(status)s",
                                   logname=logname, path=req.path) as prof:
        req.environ['monkey.profile'] = prof
        req.environ['request.uid'] = prof.uid
        resp = req.get_response(app)
        prof.extra['status'] = resp.status_int
    return resp


@wsgify.middleware
def append_time_mw(req, app):
    start_time = time.time()
    resp = req.get_response(app)
    if not resp.headers['Content-type'] == "text/plain":
        # bail out
        return resp
    end = time.time()
    time_passed = (end - start_time) 
    msg = "served in %.5f seconds" % time_passed

    slen = max(len(x) for x in resp.body.split('\n'))
    print >> resp, ''
    print >> resp, "-"*slen
    print >> resp, msg
    return resp


def timestamp(dt=None):
    if dt is None:
        dt = datetime.now()
    return "".join("%02d" %x for x in dt.timetuple()[:6])


Timer = partial(TraceProfile, uid=None, logname=None) # defanged
                                                      # TraceProfile,
                                                      # good for
                                                      # simple timing

@contextmanager
def null_trace_cm():
    """
    A do nothing context manager
    """
    yield

null_trace = null_trace_cm()

# app, cycle, port,  
