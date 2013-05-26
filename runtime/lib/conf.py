from ConfigParser import ConfigParser, NoSectionError, NoOptionError
import os

cfg = None

def __load():
    """Load application config from INI file and store in global variable"""
    global cfg
    this_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(this_dir, '..', 'etc', 'app.ini')
    cfg = ConfigParser()
    cfg.read(path)

def get(section, option, val=''):
    """Lookup value of option in section. If section or option is missing,
    then default val is returned."""
    try:
        retval = cfg.get(section, option)
    except (NoSectionError, NoOptionError):
        return val
    else:
        return retval

__load()
