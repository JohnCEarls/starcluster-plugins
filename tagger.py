from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log
import starcluster.logger
import json
from datetime import datetime
import getpass
import re

class Tagger(DefaultClusterSetup):
    """
    Plugin that adds arbitrary tags to all nodes on startup.

    Plugin sig in config like

    [plugin base-tagger]
    setup_class=tagger.Tagger
    tags = key:value, started-by:John C. Earls, ...
    """
    def __init__(self, tags, tag_date=False):
        super(Tagger, self).__init__()
        self.tags = self.parse_tags(tags)

    def parse_tags(self, tags):
        tag_dict = {}
        for pair in tags.split(','):
            try:
                key, value = pair.split(':')
                tag_dict[key.strip()] = value.strip()
            except Exception as e:
                log.exception(("Tagger error on given tags [%s] "
                        "for pair <%s>") % (tags, pair))
        log.info("Tagger: %s" % json.dumps(tag_dict))
        return tag_dict

    def add_tags(self, node):
        for key, value in self.tags.iteritems():
            log.debug("key: <%s>, value: <%s>" % 
                    (key,self.get_value(value, node)))
            node.add_tag(key,self.get_value(value,node))
        log.info('Tagged %s' % node.alias)

    def run(self, nodes, master, user, user_shell, volumes):
        for node in nodes:
            self.add_tags(node)

    def on_add_node(self, new_node, nodes, master, user, user_shell, volumes):
        self.add_tags(new_node)

    def get_value(self, value, node):
        """
        Handle variables
        [[date]]
        [[alias]] - node name
        [[master]] - name of this nodes master
        [[localuser]] - user name of person that started cluster, 
            according to machine cluster started from
        """
        auto_pattern = r'\[\[(.+)\]\]'
        auto_v = re.match(auto_pattern, value)
        if auto_v:
            command = auto_v.group(1).strip()
            if command == 'date':
                return datetime.utcnow().strftime('%c UTC')
            if command == 'alias':
                return node.alias
            if command == 'master':
                return master.alias
            if command == 'localuser':
                return getpass.getuser()
            log.error(("Tagging: <%s> appears to be a patterned tag, but " 
                    "no command found. Tagging as <%s>.") % 
                    (value, command) )
            return command
        else:
            return value
