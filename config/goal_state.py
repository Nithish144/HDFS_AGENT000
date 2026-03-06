"""
Goal State — Defines what a healthy Hadoop cluster looks like.
"""
GOAL_STATE = {
    "java_installed":       True,
    "hadoop_installed":     True,
    "hadoop_version":       "3.3.6",
    "java_home_configured": True,
    "namenode_formatted":   True,   # NEW — must format before start
    "namenode_running":     True,
    "datanode_running":     True,
    "replication_factor":   1,      # single-node cluster
    "hdfs_safemode":        False,
    "critical_log_errors":  False,
}
