"""
Hadoop AI Agent — Goal-Based Infrastructure Agent
Core loop: Detect → Compare → Reason → Validate → Execute → Repeat
"""
import json
import time
import logging
from typing import Optional
from agent.state_detector import StateDetector
from agent.goal_comparator import GoalComparator
from agent.llm_reasoner import LLMReasoner
from tools.tool_validator import ToolValidator
from tools.executor import ToolExecutor
from config.goal_state import GOAL_STATE
from config.settings import Settings

logger = logging.getLogger(__name__)


class HadoopAgent:
    def __init__(self, settings: Settings):
        self.settings        = settings
        self.state_detector  = StateDetector()
        self.goal_comparator = GoalComparator(GOAL_STATE)
        self.llm_reasoner    = LLMReasoner(settings)
        self.tool_validator  = ToolValidator()
        self.tool_executor   = ToolExecutor(dry_run=settings.dry_run)
        self.action_log      = []
        self.max_iterations  = settings.max_iterations
        self._last_result: dict = {}
        self._analyze_logs_zero_count = 0

    def _count_consecutive_tool_failures(self, tool_name: str,
                                          failure_key: str = "namenode_running") -> int:
        count = 0
        for entry in reversed(self.action_log):
            if entry.get("tool") != tool_name:
                break
            if entry.get("result", {}).get(failure_key) is True:
                break
            count += 1
        return count

    def _daemon_error_override(self) -> Optional[str]:
        daemon_error = self._last_result.get("daemon_error")
        ssh_ready    = self._last_result.get("ssh_ready")

        if ssh_ready is False:
            return (
                "OVERRIDE — SSH not ready. "
                "Call request_human_approval with reason: SSH could not be configured."
            )

        if daemon_error:
            return (
                "OVERRIDE — start_hdfs returned daemon_error. "
                "DO NOT call start_hdfs again.\n"
                f"daemon_error content:\n{daemon_error}\n"
                "Diagnose and call the correct fix tool per DAEMON FAILURE RULES."
            )

        consecutive = self._count_consecutive_tool_failures("start_hdfs", "namenode_running")
        if consecutive >= 2:
            return (
                f"OVERRIDE — start_hdfs has failed {consecutive} times in a row. "
                "Call analyze_logs now. DO NOT call start_hdfs again."
            )

        if self._analyze_logs_zero_count >= 2:
            return (
                "OVERRIDE — analyze_logs returned 0 errors "
                f"{self._analyze_logs_zero_count} times in a row. "
                "Remaining critical_log_errors are stale pre-startup entries. "
                "IGNORE critical_log_errors. Declare SUCCESS if all other goals met. "
                "DO NOT call analyze_logs again."
            )

        return None

    def run(self) -> dict:
        logger.info("🚀 Hadoop AI Agent starting...")

        # Bootstrap env vars so start/stop scripts work from any terminal
        try:
            self.tool_executor._write_daemon_users_to_hadoop_env()
            hadoop_home = self.tool_executor._hh()
            java_home   = self.tool_executor._resolve_java_home()
            self.tool_executor._write_profile_d(hadoop_home, java_home)
        except Exception as e:
            logger.warning(f"Bootstrap warning: {e}")

        iteration = 0
        while iteration < self.max_iterations:
            iteration += 1
            logger.info(f"\n{'='*50}")
            logger.info(f"🔄 Iteration {iteration}/{self.max_iterations}")

            # 1. Detect
            current_state = self.state_detector.collect()
            logger.info(f"📊 State: {json.dumps(current_state, indent=2)}")

            # 2. Compare
            gaps = self.goal_comparator.find_gaps(current_state)
            logger.info(f"🎯 Gaps: {gaps}")

            # 3. Stale-log shortcut
            if self._analyze_logs_zero_count >= 2:
                real_gaps = [g for g in gaps if g.get("field") != "critical_log_errors"]
                if not real_gaps:
                    logger.info("✅ GOAL STATE ACHIEVED (stale log errors ignored).")
                    return {"status": "success", "iterations": iteration, "log": self.action_log}

            # 3b. namenode_formatted=false while NameNode is running is now impossible —
            # executor.py _format_namenode sets dfs.namenode.name.dir in hdfs-site.xml
            # BEFORE formatting, and _configure_hdfs_site never touches that property.
            # So state_detector always reads the same path that was formatted.
            # No workaround needed here.

            # 4. Success check
            if not gaps:
                logger.info("✅ GOAL STATE ACHIEVED.")
                return {"status": "success", "iterations": iteration, "log": self.action_log}

            # 5. Override
            override = self._daemon_error_override()
            if override:
                logger.warning(f"🛑 Override: {override[:120]}...")

            # 6. LLM decision
            decision = self.llm_reasoner.decide(current_state, gaps,
                                                override_instruction=override)
            logger.info(f"🧠 Decision: {json.dumps(decision, indent=2)}")

            if not decision or "tool" not in decision:
                logger.error("❌ LLM returned invalid decision.")
                return {"status": "error", "reason": "invalid_llm_decision", "log": self.action_log}

            tool_name = decision["tool"]

            # 7. Hard-block loop guard
            if override and tool_name == "start_hdfs":
                logger.error("🚫 LLM ignored override — forcing analyze_logs.")
                decision  = {"tool": "analyze_logs", "arguments": {},
                             "reasoning": "[override] forced analyze_logs"}
                tool_name = "analyze_logs"

            # 8. Validate
            is_valid, reason = self.tool_validator.validate(decision)
            if not is_valid:
                logger.error(f"🚫 Validation failed: {reason}")
                self.action_log.append({"iteration": iteration, "action": "BLOCKED",
                                        "reason": reason})
                continue

            # 9. Execute
            arguments = decision.get("arguments", {})
            logger.info(f"⚙️  Executing: {tool_name} args={arguments}")
            result = self.tool_executor.execute(tool_name, arguments)
            logger.info(f"📋 Result: {result}")

            # 10. Track analyze_logs zero streak
            if tool_name == "analyze_logs":
                if result.get("errors_found", 1) == 0:
                    self._analyze_logs_zero_count += 1
                else:
                    self._analyze_logs_zero_count = 0
            else:
                self._analyze_logs_zero_count = 0

            # 11. Persist
            self._last_result = result
            self.action_log.append({
                "iteration":  iteration,
                "state_gaps": gaps,
                "reasoning":  decision.get("reasoning"),
                "tool":       tool_name,
                "arguments":  arguments,
                "result":     result,
            })

            time.sleep(self.settings.loop_delay_seconds)

        logger.warning("⚠️  Max iterations reached.")
        return {"status": "max_iterations_reached", "log": self.action_log}
