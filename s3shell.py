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
    def __init__(self, s3_file_path, user = None ):
        super(S3ShellPlugin, self).__init__()
        parsed = s3_file_path[5:].split('/')
        self.bucket = parsed[0]
        self.path = '/'.join(parsed[1:])
        self._user = user

    def run(self, nodes, master, user, user_shell, volumes):
        ctr = 0
        complete = False

        while not complete and ctr < 3:
            if ctr > 0:
                log.info( "Retrying initialization")
            try:
                if self._user != 'root':
                    self.run_scripts( master, user )
                else:
                    self.run_scripts_root( master )
                complete = True
            except:
                log.exception("!!!S3shell plugin failed!!!")
                ctr += 1
        if not complete:
            log.error("Unable to run S3shell.  Tried %i times." % ctr)
            #this plugin failing should not stop startup

    def run_once( self, master_ssh, cmd ):
        log.info("Running %s on master." % cmd)
        master_ssh.execute( cmd , silent=False )
        log.info("%s complete." % cmd)

    def run_scripts(self, master, user ):
        _, script =  os.path.split(self.path)
        cmd1 = 'aws s3 --region=us-east-1 cp s3://%s/%s /home/%s/%s' %\
                (self.bucket, self.path, user, script)
        cmd2 = 'bash /home/%s/%s &>> bootstrap.log' %  (  user, script )
        cmds = (cmd1, cmd2)
        mssh = master.ssh
        mssh.switch_user(user)
        for cmd in cmds:
            self.run_once( mssh, cmd )

    def run_scripts_root(self, master ):
        _, script =  os.path.split(self.path)
        cmd1 = 'aws s3 --region=us-east-1 cp s3://%s/%s /root/%s' %\
                (self.bucket, self.path, script)
        cmd2 = 'bash /root/%s &>> bootstrap.log' %  (  script, )
        cmds = (cmd1, cmd2)
        mssh = master.ssh
        mssh.switch_user('root')
        for cmd in cmds:
            self.run_once( mssh, cmd )
