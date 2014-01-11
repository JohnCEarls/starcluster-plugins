from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log
import starcluster.logger
import json
from datetime import datetime
import getpass
import re
import os.path
from starcluster import exception

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

    def run(self, nodes, master, user, user_shell, volumes):
        ctr = 0
        complete = False
        while not complete and ctr < 3:
            if ctr > 0:
                log.info( "Retrying initialization")
            try:
                self.run_scripts( nodes, user )
                complete = True
            except exception.TheadPoolException as t:
                log.exception("Threadpool exception, try without threads")
                self.run_scripts( nodes, user, True )
            except:
                log.exception("!!!S3shell plugin failed!!!")
                ctr += 1
        if not complete:
            log.error("Unable to run S3shell.  Tried %i times." % ctr)
                #this plugin failing should not stop startup

    def run_scripts(self, nodes, user, noThreads=False):
        _, script =  os.path.split(self.path)
        cmd1 = 'aws s3 --region=us-east-1 cp s3://%s/%s /home/%s/%s' %\
                (self.bucket, self.path, user, script)
        log.info("Running %s on all nodes." % cmd1)
        for node in nodes:
            nssh = node.ssh
            nssh.switch_user(user)
            if noTheads:
                nssh.execute( cmd1 )
            else:
                self.pool.simple_job( nssh.execute, (cmd1,), jobid= node.alias)
        if not noThreads:
            self.pool.wait(len(nodes))
        cmd2 = 'bash /home/%s/%s &> user-bootstrap.log' %  ( user, script )
        log.info("Running $ %s on all nodes." % cmd2)
        for node in nodes:
            nssh = node.ssh
            nssh.switch_user(user)
            if noTheads:
                nssh.execute( cmd1 )
            else:
                self.pool.simple_job( nssh.execute, (cmd2,), jobid= node.alias)
        if not noThreads:
            self.pool.wait(len(nodes))

    def on_add_node(self, new_node, nodes, master, user, user_shell, volumes):
        ctr = 0
        complete = False
        while not complete and ctr < 3:
            if ctr > 0:
                log.info( "Retrying initialization")
            try:
                self.run_scripts( [new_node], user )
                complete = True
            except:
                log.exception("!!!S3shell plugin failed!!!")
                #this plugin failing should not stop startup
                ctr += 1
        if not complete:
            log.error("Unable to run S3shell.  Tried %i times." % ctr)
                #this plugin failing should not stop startup
