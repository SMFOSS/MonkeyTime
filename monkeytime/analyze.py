#from gevent import monkey
#from gevent.pool import Pool
#import gevent
#import redis
#monkey.patch_all()
from StringIO import StringIO
from dateutil.parser import parse
from itertools import count
from melk.util.dibject import Dibject as dibj
from operator import itemgetter
from path import path
from statlib import stats
import csv
import json
import optparse
import sys
import time
import traceback

get0 = itemgetter(0)


def format_tb():
    et, ev, tb = sys.exc_info()
    handle = StringIO()
    traceback.print_exception(et, ev, tb, file=handle)
    return handle.getvalue()


def perf_parse_options(argv=None, usage="usage: %prog [options] dir"):
    parser = optparse.OptionParser(usage=usage)

    parser.add_option('--noload',
                      action="store_false",
                      help='Just count and exist',
                      dest='load',
                      default=True
                      )

    parser.add_option('-n', '--howmany',
                      type="int",
                      help='Number of lines to load',
                      dest='howmany',
                      default=10000
                      )

    if argv is None:
        argv = sys.argv
    options, args = parser.parse_args(argv)

    howmany = len(args)
    args = [path(x) for x in args]
    if howmany == 1:
        return options, path('.')
    return options, args[1]


class PerfInfo(dibj):
    """
    Performance info object
    """
    @classmethod
    def from_line(cls, line):
        return cls(**cls.parse_line(line))
        
    def __str__(self):
        return self.line

    @staticmethod
    def parse_line(line):
        #pid, timestamp, inout, lineage, howlong, uri, status = line.split()
        ele = line.split()
        if len(ele) == 6:
            pid, timestamp, inout, lineage, howlong, uri = ele
            old = True
        else:
            pid, timestamp, inout, lineage, howlong, uri, status = ele
            status = int(status)
        howlong = float(howlong)
        dt = parse(timestamp)
        origin, parent, uid = lineage.split(":")
        return locals()

## for logfile in path.files(pattern):
##     pool.spawn(load(logfile.lines))

def generate_perf_info(lines, marker=None, **filters):
    """
    Marker signal, useful when filtration removes all
    entries
    """
    counter = count()
    filtered = count()
    returned = count()
    print "Total: %s" %len(lines)
    for line in lines:
        line = line.strip()
        if line:
            try:
                perfinfo = PerfInfo.from_line(line)
                if perfinfo.get('old'):
                    next(counter)
                    continue
                
                use = True
                for key, value in filters.items():
                    ofin = perfinfo.get(key)
                    if value(ofin):
                        next(filtered)
                        use = False
                        break
                    
                if not use:
                    continue

                jsonable = perfinfo.copy()
                perfinfo['dt'] = jsonable.pop('dt')
                serialized = json.dumps(jsonable)
                next(returned)
                yield perfinfo.uid, perfinfo, serialized
            except ValueError, e:
                print "Parse error: %s" %e
                print line
                
    print "Returned: %s" %returned
    if not next(returned):
        yield marker, marker, marker
    print "Old: %s" %counter
    print "Filtered: %s" %filtered



def groups(lines, interval, **filters):
    group = []
    currtime = None
    start = None
    marker = object()
    for uid, info, blob in generate_perf_info(lines, marker=marker, **filters):
        if uid is marker: # got nothing
            break

        if currtime is None:
            currtime = info.dt
            start = info.timestamp

        group.append(info)
        depth = (info.dt - currtime).seconds

        if interval < depth:
            currtime = None
            out = group[:]
            group = []
            yield out, start

    # yield whatever is left
    yield group, start

        
def statdict(times, start, interval):
    standard_deviation = 0
    if len(times) > 1:
        standard_deviation = stats.lstdev(times)
    return dibj(interval=interval,
                start=start,
                max=max(times) * 1000,
                min=min(times) * 1000,
                mean=stats.lmean(times) * 1000,
                median=stats.lmedianscore(times) * 1000,
                p90th=abs(stats.lscoreatpercentile(times, 0.9)) * 1000,
                p98th=abs(stats.lscoreatpercentile(times, 0.98)) * 1000,
                p99th=abs(stats.lscoreatpercentile(times, 0.99)) * 1000,
                standard_deviation=standard_deviation * 1000,
                howmany=len(times))


def ext2int(ext):
    nada, ext = ext.split('.')
    if ext == 'log':
        return 0
    return int(ext)

def yield_stats(logdir, pattern="perf.log", interval=60, **filters):

    files = logdir.files(pattern)

    print "%d files" %len(files)
    
    files = sorted(((ext2int(x.ext), x) for x in files if not x.endswith('~')), key=get0, reverse=True)
    files = [y for x, y in files]
    last = None
    for logfile in files:
        lines = logfile.lines()
        print "File: %s" %logfile
        for i, (rawgroup, start) in enumerate(groups(lines, interval, **filters)):
            if not start:
                continue
            time = parse(start)
            if not i % 100:
                print time.strftime("%Y-%m-%dT%H:%M:%SZ")
            if last is None:
                last = time
                print "New start: %s" %start
            if  last > time:
                print "timewarp: %s %s" %(last.strftime("%Y-%m-%dT%H:%M:%SZ"), time.strftime("%Y-%m-%dT%H:%M:%SZ"))
                last = None

            try:
                times = [x.howlong for x in rawgroup]
                if not times:
                    continue
                yield statdict(times, start, interval)
            except Exception:
                print format_tb()


def stats_to_csv(outfile, logdir, interval, **filters):
    counter = count()

    statg = yield_stats(logdir, interval=interval, **filters)
    try:
        first = next(statg)
    except StopIteration:
        return counter
        
    fields = sorted(first)
    #print ",".join(fields)
    outfile.write_lines([",".join(fields)])
    writer = csv.DictWriter(open(outfile, 'a'), fields)
    
    for statinfo in sorted(statg, key=itemgetter('start')):
        next(counter)
        writer.writerow(statinfo)
    return counter


def mkepoch(txt, ms=False):
    dt = txt
    if isinstance(txt, basestring):
        dt = parse(txt)
    epoch = time.mktime(dt.timetuple())
    if ms:
        epoch = epoch * 1000
    return epoch


def noop(val):
    return val


def c2xy(fp, x, y, xtrans=noop, ytrans=noop, separate=True):
    """
    cvs to json array
    """
    outy, outx = [], []
    for rowd in csv.DictReader(open(fp)):
        outy.append(ytrans(rowd[y]))
        outx.append(xtrans(rowd[x]))
    if separate:
        return outx, outy
    return zip(outx, outy)



    


    
## def load_perf(argv=None, pattern="perf.log*"):
##     counter = count()
##     collision_count = count()
##     options, path = perf_parse_options(argv)
##     rdis = redis.Redis("localhost")
##     collisions = next(collision_count)
##     inlog = []
##     outlog = []
    
##     for logfile in path.files(pattern):
##         pipe = rdis.pipeline()
##         for uid, info, blob in generate_perf_info(logfile.lines()):
##             i = next(counter)
##             if info.inout == "[instr.inbound]":
##                 inlog.append((info.timestamp, info.howlong))
##             if info.inout == "[instr.outbound]":                
##                 outlog.append((info.timestamp, info.howlong))
##             if i == options.howmany:
##                 break
##             if not i % 1000 and i > 0:
##                 print info.timestamp
##                 pipe.execute()
##                 pipe = rdis.pipeline()
##             if options.load == True:
##                 name = "request:%s" %uid
##                 if rdis.exists(name):
##                     collisions = next(collision_count)
##                     continue
##                 rload(pipe, uid, name, info)
##             if not i % 100000:
##                 print "line: %s" %i
##         pipe.execute()

##     print "<<< %d lines loaded, %d collisions >>>" %(i, collisions)


## def rload(pipe, uid, name, info):
##     pipe.set(name, info.howlong)
##     ts = int("".join(info.timestamp.split(":")))
##     pipe.zadd(info.inout, name, ts)
##     for key, value in info.items():
##         pipe.hset("info:%s" %uid, key, value)
##     if info.origin:
##         pipe.hset("request:uid->origin", uid, info.origin)
##     else:
##         pipe.hset("request:uid->origin", uid, uid)
##     if info.parent:
##         pipe.hset("request:uid->parent", uid, info.parent)
##     origin = info.origin and info.origin or uid
##     pipe.zadd("request:uri_length", "%s %s" %(info.uri, float(info.howlong)), float(info.howlong))
##     pipe.zadd("request:uri_ts", "%s %s" %(info.uri, float(info.howlong)), ts)
##     pipe.zadd("request:ts_length:%s" %info.inout, "%s:%s" %(ts, float(info.howlong)), ts)
##     pipe.zadd("request:uid", uid, ts)
##     pipe.zadd("request:logged:origin:%s" %origin, uid, ts)
##     pipe.zadd("request:logged:parent:%s" %info.parent, uid, ts)
##     pipe.zadd("request:logged:id:%s" %origin, uid, ts)
##     pipe.zadd("request:howlong:id:%s" %origin, uid, float(info.howlong))
##     pipe.zadd("request:howlong:uri:%s" %info.uri, float(info.howlong), float(info.howlong))
##     pipe.zadd("request:howlong:uids_for_uri:%s" %info.uri, uid, float(info.howlong))
##     pipe.zadd("request:howlong:origins_for_uri:%s" %info.uri, info.origin, float(info.howlong))
##     pipe.zadd("request:when:origins_for_uri:%s" %info.uri, info.origin, ts)
##     if info.inout == "[inbound]":
##         pipe.hincrby("request:uri_hits", info.uri)
##         pipe.zincrby("request:uri_hit_set", info.uri)


 
## def analyze(rdis):
##     in_uids = (x.split(":")[1] for x in rdis.zrange("[instr.inbound]", 0, 10000000))
##     out_uids = (x.split(":")[1] for x in rdis.zrange("[instr.outbound]", 0, 10000000))
##     in_names = (x for x in rdis.zrange("[instr.inbound]", 0, 10000000))
##     out_names = (x for x in rdis.zrange("[instr.outbound]", 0, 10000000))
##     in_times = [float(rdis.get(x)) for x in in_names]
##     out_times = [float(rdis.get(x)) for x in out_names]
##     def statdict(times):
##         return dict(max=max(times),
##                     min=min(times),
##                     p90=abs(stats.lscoreatpercentile(times, 0.1)),
##                     p99=abs(stats.lscoreatpercentile(times, 0.01)),
##                     dev=stats.stdev(times))
##     inbound = statdict(in_times)
##     outbound = statdict(out_times)
    

    

## >>> meanin = stats.mean(in_times)
## indev = stats.stdev(in_times)
## out90 = [float(x) for x in out_times], .1)
## in99 = stats.lscoreatpercentile([float(x) for x in in_times], .01)
