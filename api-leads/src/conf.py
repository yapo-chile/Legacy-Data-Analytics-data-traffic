import sys

class Conf(object):

    def __init__(self, conf_file):
        self.conf_file = conf_file
        self.conf = Conf.load_conf(conf_file)

    def get(self, key):
        return self.conf.get(key)

    def reload(self):
        self.conf = Conf.load_conf(self.conf_file)

    @staticmethod
    def load_conf(filename):
        """Initializes conf module so values can be accessed as conf.var"""
        conf = {}
        try:
            with open(filename, 'r') as f:
                for line in f.readlines():
                    if line.strip() and not line.startswith('#'):
                        k, v = line.strip().split('=', 1)
                        conf[k] = v
        except IOError as e:
            print >> sys.stderr, "{0}: Error loading configuration file\n{1}".format(
                sys.argv[0], e)
            sys.exit(1)
        return conf
