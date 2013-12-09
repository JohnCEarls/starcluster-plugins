starcluster-plugins
===================

collection of my starcluster plugins

tagger.py
---------
Starcluster plugin that adds tags to ec2 instances on startup

Copy to plugins folder and add similar to config

    [plugin base-tagger]
    setup_class=tagger.Tagger
    tags = key:value, started-by:John C. Earls, project: testing,  mydate:[[date]], myalias:  [[alias]], localuser:[[localuser]], 

Has a few variables that can be used as values.
- [[date]] - datetime in UTC
- [[alias]] - node name
- [[localuser]] - users name on computer that is starting the cluster
- [[master]] - name of the master node for this starcluster
