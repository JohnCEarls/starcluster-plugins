from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log
import starcluster.logger
import json
from datetime import datetime
import getpass
import re
import os.path

class S3ShellPlugin(DefaultClusterSetup):
    """
    Plugin that downloads and executes a shell script from s3 as cluster user
    On all nodes
    [plugin userinit]
    s3_file_path = s3://mybucket/path/to/script.sh
    """
    def __init__(self, s3_file_path ):
        super(S3ShellPlugin, self).__init__()
        parsed = s3_file_path[5:].split('/')
        self.bucket = parsed[0]
        self.path = '/'.join(parsed[1:])

    def run( nodes, master, user, user_shell, volumes):
        self.run_scripts( nodes, user )

    def run_scripts(self, nodes, user):
        log.info("Running %s on all nodes.")
        _, script =  os.path.split(self.path)
        cmd1 = 'aws --region=us-east-1 cp s3://%s/%s /home/%s/%s' %\
                (self.bucket, self.path, user, script)
        log.info("Running %s on all nodes." % cmd1)
        for node in nodes:
            nssh = node.ssh
            nssh.switch_user(user)
            self.pool.simple_job( nssh.execute, (cmd1,), jobid= node.alias)
        self.pool.wait(len(nodes))
        cmd2 = 'bash /home/%s/%s' %  ( user, script )
        log.info("Running $ %s on all nodes." % cmd2)
        for node in nodes:
            nssh = node.ssh
            nssh.switch_user(user)
            self.pool.simple_job( nssh.execute, (cmd2,), jobid= node.alias)
        self.pool.wait(len(nodes))

    def on_add_node(self, new_node, nodes, master, user, user_shell, volumes):
        self.run_scripts( [new_node], user )
