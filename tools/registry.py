"""
Tool Registry — Single source of truth for all allowed tools.
"""

TOOL_REGISTRY = {
    # ── Installation ────────────────────────────────────────────────────────
    "install_java": {
        "description": "Install OpenJDK on the system.",
        "required_args": ["version"],
        "allowed_args": {"version": ["11", "17", "21"]},
    },
    "install_hadoop": {
        "description": "Download and install Apache Hadoop.",
        "required_args": ["version"],
        "allowed_args": {"version": ["3.3.6", "3.4.0"]},
    },
    # ── Configuration ───────────────────────────────────────────────────────
    "configure_java_home": {
        "description": "Set JAVA_HOME in hadoop-env.sh.",
        "required_args": [],
        "allowed_args": {},
    },
    "configure_hdfs_site": {
        "description": "Update hdfs-site.xml with replication factor and data dirs.",
        "required_args": ["replication_factor"],
        "allowed_args": {"replication_factor": [1, 2, 3]},
    },
    "configure_core_site": {
        "description": "Update core-site.xml with NameNode URI (fs.defaultFS=hdfs://localhost:9000).",
        "required_args": [],
        "allowed_args": {},
    },
    # ── Lifecycle ───────────────────────────────────────────────────────────
    "format_namenode": {
        # FIX: removed from DESTRUCTIVE_TOOLS and removed human_approved requirement
        # so agent can call it automatically when namenode_formatted=False
        "description": "Format the NameNode. Safe when namenode_formatted=false (no data exists).",
        "required_args": [],
        "allowed_args": {},
    },
    "start_hdfs": {
        "description": "Start HDFS daemons (NameNode + DataNode).",
        "required_args": [],
        "allowed_args": {},
    },
    "stop_hdfs": {
        "description": "Gracefully stop HDFS.",
        "required_args": [],
        "allowed_args": {},
    },
    "restart_namenode": {
        "description": "Restart only the NameNode daemon.",
        "required_args": [],
        "allowed_args": {},
    },
    "restart_datanode": {
        "description": "Restart only the DataNode daemon.",
        "required_args": [],
        "allowed_args": {},
    },
    "leave_safemode": {
        "description": "Force HDFS out of safe mode.",
        "required_args": [],
        "allowed_args": {},
    },
    # ── Health & Diagnostics ────────────────────────────────────────────────
    "check_hdfs_health": {
        "description": "Run hdfs dfsadmin -report.",
        "required_args": [],
        "allowed_args": {},
    },
    "analyze_logs": {
        "description": "Scan Hadoop logs for recent ERROR/FATAL entries.",
        "required_args": [],
        "allowed_args": {},
    },
    "check_disk_space": {
        "description": "Check available disk space.",
        "required_args": [],
        "allowed_args": {},
    },
    # ── Human Escalation ────────────────────────────────────────────────────
    "request_human_approval": {
        "description": "Pause agent and request human intervention.",
        "required_args": ["reason"],
        "allowed_args": {},
    },
}

# FIX: format_namenode removed — it must be callable automatically.
# stop_hdfs remains destructive (data-safe restart requires human awareness).
DESTRUCTIVE_TOOLS = {
    "stop_hdfs",
}
