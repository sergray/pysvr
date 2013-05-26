"""Consolidated logging of processes running on the same machine via UDP.

Provides logging functions and implements UDP server, writing log messages
to the file. Script execution starts server."""
# import python modules
import sys, os, resource, socket
from time import gmtime
# import custom modules
import conf

# global variables
sock, dstaddr = None, None

def __init():
    """Initialize module. Establish UDP connection with remote end,
    given in config."""
    global sock, dstaddr
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    port = conf.get('plog', 'port')
    dstaddr = ('localhost', int(port))

def date_str(time_tuple):
    "Return date string for given time_tuple"
    return "%04d/%02d/%02d" % (
        time_tuple.tm_year, time_tuple.tm_mon, time_tuple.tm_mday)

def time_str(time_tuple):
    "Return time string for given time_tuple"
    return "%02d:%02d:%02d" % (
        time_tuple.tm_hour, time_tuple.tm_min, time_tuple.tm_sec)


def __send(level, message):
    "Send logging message string with level given as string to remote service"
    gmt = gmtime()
    pkt = ' '.join([date_str(gmt), time_str(gmt), level, '-', message])
    sock.sendto(pkt, dstaddr)

def error(msg):
    "Send ERROR msg"
    __send('ERROR', msg)

def info(msg):
    "Send INFO msg"
    __send('INFO ', msg)

def debug(msg):
    "Send DEBUG msg"
    __send('DEBUG', msg)

def __daemonize():
    "Daemonize current process"
    # do the UNIX double-fork magic, see Stevens' "Advanced
    # Programming in the UNIX Environment" for details (ISBN 0201563177)
    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            os.wait()   # wait for second parent
            sys.exit(0)
    except OSError, exc:
        print >> sys.stderr, "fork #1 failed: %d (%s)" % (
            exc.errno, exc.strerror)
        sys.exit(1)

    # decouple from parent environment
    os.setsid()
    os.umask(0)

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent, print eventual PID before
            # print pid
            sys.exit(0)

    except OSError, exc:
        print >> sys.stderr, "fork #2 failed: %d (%s)" % (
            exc.errno, exc.strerror)
        sys.exit(1)

    #change to data directory if needed
    if os.setsid() == -1:
        sys.exit('setsid failed')
    if os.umask(0) == -1:
        sys.exit('umask failed')

    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = 1024

    # Iterate through and close all file descriptors.
    sys.stdin.close()
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError: # ERROR, fd wasn't open to begin with (ignored)
            pass


def run(appname):
    sock.bind(dstaddr)
    dstdir = conf.get('plog', 'logdir')
    print 'plog udp server: listening to port', dstaddr[1]
    print 'plog udp server: writing to', dstdir

    os.chdir(dstdir)

    def mkfname(tm):
        return 'app-%04d%02d%02d.log' % (tm.tm_year, tm.tm_mon, tm.tm_mday)

    ftime = gmtime()
    fname = mkfname(ftime)
    fobj = open(fname, 'a+b')
    fobj.write('%s %s INFO  plog started; app=%s\n' % (
        date_str(ftime), time_str(ftime), appname))
    fobj.flush()
    
    while 1:
        pkt, addr = sock.recvfrom(1024*8)
        if not pkt:
            break
        now = gmtime()
        if now.tm_mday != ftime.tm_mday or now.tm_mon != ftime.tm_mon:
            oldfname = fname
            fobj.close()

            # open new file
            ftime = now
            fname = mkfname(ftime)
            fobj = open(fname, 'a+b')

            # gzip old file in the background
            os.system("nohup gzip '%s' &" % oldfname)
            
        fobj.write(pkt)
        fobj.write('\n')
        fobj.flush()


__init()
if __name__ == '__main__':
    pidpath = conf.get('plog', 'pidpath', '')
    if not pidpath:
        sys.exit('Error: missing pidpath in app.ini')

    # go to background
    __daemonize()

    # save our pid 
    with open(pidpath, 'w') as fp:
        fp.write('%d\n' % os.getpid())

    # need to call init again because daemonize closed all FDs
    __init()  

    # run the loop
    run(len(sys.argv) > 1 and sys.argv[1] or '')
