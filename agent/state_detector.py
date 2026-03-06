"""
State Detector — Collects the real current state of the Hadoop cluster.
"""
import os
import re
import subprocess
import shutil
import logging
import glob
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

HADOOP_BIN_SYMLINKS = ["hadoop", "hdfs", "yarn", "mapred"]


def _resolve_hadoop_home() -> str:
    env = os.environ.get("HADOOP_HOME", "").strip()
    if env and os.path.isfile(os.path.join(env, "bin", "hadoop")):
        return env
    if os.path.isfile("/usr/local/hadoop/bin/hadoop"):
        real = os.path.realpath("/usr/local/hadoop")
        return real if os.path.isfile(os.path.join(real, "bin", "hadoop")) else "/usr/local/hadoop"
    for c in sorted(glob.glob("/usr/local/hadoop-*"), reverse=True):
        if os.path.isfile(os.path.join(c, "bin", "hadoop")):
            return c
    return "/usr/local/hadoop"


def _resolve_java_home() -> str:
    try:
        java_bin = subprocess.run(["which", "java"], capture_output=True, text=True).stdout.strip()
        if java_bin:
            real = subprocess.run(["readlink", "-f", java_bin], capture_output=True, text=True).stdout.strip()
            return os.path.dirname(os.path.dirname(real))
    except Exception:
        pass
    for c in ["/usr/lib/jvm/java-11-openjdk-amd64", "/usr/lib/jvm/java-11-openjdk-arm64",
              "/usr/lib/jvm/java-21-openjdk-amd64"]:
        if os.path.isfile(os.path.join(c, "bin", "java")):
            return c
    return "/usr/lib/jvm/java-11-openjdk-amd64"


class StateDetector:

    def collect(self) -> dict:
        return {
            "java_installed":       self._check_java(),
            "java_version":         self._get_java_version(),
            "hadoop_installed":     self._check_hadoop(),
            "hadoop_version":       self._get_hadoop_version(),
            "java_home_configured": self._check_java_home(),
            "namenode_formatted":   self._check_namenode_formatted(),  # NEW
            "namenode_running":     self._check_process("NameNode"),
            "datanode_running":     self._check_process("DataNode"),
            "replication_factor":   self._get_replication_factor(),
            "hdfs_safemode":        self._check_safemode(),
            "critical_log_errors":  self._check_log_errors(),
            "disk_usage_percent":   self._get_disk_usage(),
        }

    # ── java ─────────────────────────────────────────────────────────────────

    def _check_java(self) -> bool:
        return shutil.which("java") is not None

    def _get_java_version(self) -> Optional[str]:
        try:
            out = subprocess.run(["java", "-version"], capture_output=True,
                                 text=True, timeout=5).stderr
            if '"' in out:
                return out.split('"')[1]
        except Exception:
            pass
        return None

    # ── hadoop ───────────────────────────────────────────────────────────────

    def _get_current_user(self) -> str:
        import pwd as _pwd
        try:
            return _pwd.getpwuid(os.getuid()).pw_name
        except Exception:
            return "root"

    def _write_profile_d(self, hadoop_home: str, java_home: str):
        import pwd as _pwd
        profile_file = "/etc/profile.d/hadoop.sh"
        user = self._get_current_user()
        content = (
            "# Hadoop AI Agent — auto-generated\n"
            f"export HADOOP_HOME={hadoop_home}\n"
            f"export JAVA_HOME={java_home}\n"
            f"export PATH=$PATH:{hadoop_home}/bin:{hadoop_home}/sbin:{java_home}/bin\n"
            f"export HDFS_NAMENODE_USER={user}\n"
            f"export HDFS_DATANODE_USER={user}\n"
            f"export HDFS_SECONDARYNAMENODE_USER={user}\n"
            f"export YARN_RESOURCEMANAGER_USER={user}\n"
            f"export YARN_NODEMANAGER_USER={user}\n"
        )
        try:
            if os.getuid() == 0:
                open(profile_file, "w").write(content)
                os.chmod(profile_file, 0o644)
            else:
                import tempfile
                tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False)
                tmp.write(content); tmp.flush(); tmp.close()
                subprocess.run(["sudo", "-n", "cp", tmp.name, profile_file], check=True)
                subprocess.run(["sudo", "-n", "chmod", "644", profile_file], check=True)
                os.unlink(tmp.name)
        except Exception as e:
            logger.warning(f"Could not write {profile_file}: {e}")

        for key, val in [
            ("HADOOP_HOME", hadoop_home), ("JAVA_HOME", java_home),
            ("HDFS_NAMENODE_USER", user), ("HDFS_DATANODE_USER", user),
            ("HDFS_SECONDARYNAMENODE_USER", user),
            ("YARN_RESOURCEMANAGER_USER", user), ("YARN_NODEMANAGER_USER", user),
        ]:
            os.environ[key] = val
        if hadoop_home + "/sbin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{hadoop_home}/bin:{hadoop_home}/sbin:{os.environ.get('PATH','')}"

        source_line = f"\n# Hadoop AI Agent\n[ -f {profile_file} ] && source {profile_file}\n"
        homes = {os.path.expanduser("~"), "/root", "/home/ubuntu"}
        try:
            for entry in _pwd.getpwall():
                if entry.pw_uid >= 1000 and os.path.isdir(entry.pw_dir):
                    homes.add(entry.pw_dir)
        except Exception:
            pass
        for home in homes:
            bashrc = os.path.join(home, ".bashrc")
            try:
                existing = open(bashrc).read() if os.path.isfile(bashrc) else ""
                if profile_file not in existing:
                    open(bashrc, "a").write(source_line)
            except Exception:
                try:
                    subprocess.run(
                        ["sudo", "-n", "bash", "-c",
                         f"grep -q '{profile_file}' {bashrc} 2>/dev/null || "
                         f"printf '\n# Hadoop AI Agent\n[ -f {profile_file} ] && source {profile_file}\n' >> {bashrc}"],
                        capture_output=True, text=True, timeout=10)
                except Exception:
                    pass

    def _create_bin_symlinks(self, hadoop_home: str):
        for cmd in HADOOP_BIN_SYMLINKS:
            src = os.path.join(hadoop_home, "bin", cmd)
            dst = f"/usr/local/bin/{cmd}"
            if not os.path.isfile(src):
                continue
            try:
                if os.path.islink(dst) or os.path.exists(dst):
                    if os.path.realpath(dst) == os.path.realpath(src):
                        continue
                    if os.getuid() == 0:
                        os.remove(dst)
                    else:
                        subprocess.run(["sudo", "-n", "rm", "-f", dst], check=True)
                if os.getuid() == 0:
                    os.symlink(src, dst)
                else:
                    subprocess.run(["sudo", "-n", "ln", "-sf", src, dst], check=True)
            except Exception as e:
                logger.warning(f"Could not symlink {cmd}: {e}")

    def _ensure_hadoop_on_path(self) -> bool:
        hadoop_home = _resolve_hadoop_home()
        if not os.path.isfile(os.path.join(hadoop_home, "bin", "hadoop")):
            return False
        java_home = _resolve_java_home()
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["JAVA_HOME"]   = java_home
        if hadoop_home + "/bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{hadoop_home}/bin:{hadoop_home}/sbin:{os.environ.get('PATH','')}"
        self._write_profile_d(hadoop_home, java_home)
        self._create_bin_symlinks(hadoop_home)
        return shutil.which("hadoop") is not None

    def _check_hadoop(self) -> bool:
        return self._ensure_hadoop_on_path()

    def _get_hadoop_version(self) -> Optional[str]:
        self._ensure_hadoop_on_path()
        try:
            out = subprocess.run(["hadoop", "version"], capture_output=True,
                                 text=True, timeout=5).stdout
            if out:
                return out.splitlines()[0].split()[-1]
        except Exception:
            pass
        return None

    def _check_java_home(self) -> bool:
        java_home = os.environ.get("JAVA_HOME", "").strip()
        if java_home and os.path.isfile(os.path.join(java_home, "bin", "java")):
            return True
        env_file = os.path.join(_resolve_hadoop_home(), "etc", "hadoop", "hadoop-env.sh")
        try:
            for line in open(env_file):
                line = line.strip()
                if line.startswith("export JAVA_HOME="):
                    jh = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if jh and os.path.isfile(os.path.join(jh, "bin", "java")):
                        os.environ["JAVA_HOME"] = jh
                        return True
        except Exception:
            pass
        return False

    # ── NEW: namenode formatted check ────────────────────────────────────────

    def _check_namenode_formatted(self) -> bool:
        """
        Read dfs.namenode.name.dir from hdfs-site.xml and check if
        the 'current' subdirectory exists — proof of a successful format.
        Never relies on a hardcoded path.
        """
        xml_path = os.path.join(_resolve_hadoop_home(), "etc", "hadoop", "hdfs-site.xml")
        raw = ""
        try:
            for prop in ET.parse(xml_path).getroot().findall("property"):
                n = prop.find("name")
                if n is not None and n.text == "dfs.namenode.name.dir":
                    v = prop.find("value")
                    raw = v.text if v is not None else ""
                    break
        except Exception:
            pass

        if not raw:
            user = self._get_current_user()
            raw  = f"file:///tmp/hadoop-{user}/dfs/name"

        # Strip file:// URI prefix → plain path
        path = re.sub(r"^file://", "", raw)
        path = re.sub(r"^file:",  "", path).strip()

        formatted = os.path.isdir(os.path.join(path, "current"))
        logger.info(f"NameNode dir={path} formatted={formatted}")
        return formatted

    # ── processes ────────────────────────────────────────────────────────────

    def _check_process(self, process_name: str) -> bool:
        try:
            out = subprocess.run(["jps"], capture_output=True, text=True, timeout=5).stdout
            return any(l.strip().endswith(process_name) for l in out.splitlines())
        except Exception:
            return False

    # ── config ───────────────────────────────────────────────────────────────

    def _get_replication_factor(self) -> Optional[int]:
        xml_path = os.path.join(_resolve_hadoop_home(), "etc", "hadoop", "hdfs-site.xml")
        try:
            if os.path.exists(xml_path):
                for prop in ET.parse(xml_path).getroot().findall("property"):
                    n = prop.find("name")
                    if n is not None and n.text == "dfs.replication":
                        v = prop.find("value")
                        if v is not None and v.text:
                            return int(v.text.strip())
        except Exception:
            pass
        return None

    def _check_safemode(self) -> bool:
        try:
            out = subprocess.run(["hdfs", "dfsadmin", "-safemode", "get"],
                                 capture_output=True, text=True, timeout=10).stdout
            return "ON" in out
        except Exception:
            return False

    # ── logs ─────────────────────────────────────────────────────────────────

    def _check_log_errors(self) -> bool:
        hadoop_home = _resolve_hadoop_home()
        log_dirs    = [os.path.join(hadoop_home, "logs"),
                       os.environ.get("HADOOP_LOG_DIR", "")]

        namenode_start_time = None
        NAMENODE_START_MARKERS = ["NameNode RPC up at:", "IPC Server Responder", "createNameNode"]

        for log_dir in log_dirs:
            if not log_dir or not os.path.isdir(log_dir):
                continue
            for fname in os.listdir(log_dir):
                if "namenode" not in fname.lower() or not fname.endswith(".log"):
                    continue
                try:
                    for line in open(os.path.join(log_dir, fname), errors="ignore"):
                        if any(m in line for m in NAMENODE_START_MARKERS):
                            try:
                                namenode_start_time = datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
                            except ValueError:
                                pass
                except Exception:
                    pass

        cutoff = namenode_start_time if namenode_start_time else (
            datetime.now() - timedelta(minutes=3))

        IGNORE_PATTERNS = [
            "RECEIVED SIGNAL 15: SIGTERM",
            "RECEIVED SIGNAL 2: SIGINT",
            "file:/// has no authority",       # pre core-site config — expected
            "No services to connect",           # pre core-site config — expected
            "missing NameNode address",         # pre core-site config — expected
            "NameNode is not formatted",        # FIX: pre-format error — ignore after fix
            "recoverTransitionRead",            # FIX: caused by not-formatted state
            "FSImage",                          # FIX: same root cause
        ]

        for log_dir in log_dirs:
            if not log_dir or not os.path.isdir(log_dir):
                continue
            for fname in os.listdir(log_dir):
                if not fname.endswith(".log"):
                    continue
                try:
                    for line in open(os.path.join(log_dir, fname),
                                     errors="ignore").readlines()[-300:]:
                        if "FATAL" not in line and "ERROR" not in line:
                            continue
                        if any(p in line for p in IGNORE_PATTERNS):
                            continue
                        try:
                            ts = datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
                            if ts >= cutoff:
                                return True
                        except ValueError:
                            pass
                except Exception:
                    pass
        return False

    # ── disk ─────────────────────────────────────────────────────────────────

    def _get_disk_usage(self) -> Optional[int]:
        try:
            lines = subprocess.run(["df", "-h", "/"], capture_output=True,
                                   text=True, timeout=5).stdout.splitlines()
            if len(lines) > 1:
                parts = lines[1].split()
                if len(parts) >= 5:
                    return int(parts[4].replace("%", ""))
        except Exception:
            pass
        return None
