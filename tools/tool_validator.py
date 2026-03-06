"""
Tool Validator — Safety layer that validates every LLM tool decision
before execution. Blocks destructive or unknown operations.
"""
import logging
from tools.registry import TOOL_REGISTRY, DESTRUCTIVE_TOOLS

logger = logging.getLogger(__name__)


class ToolValidator:
    def validate(self, decision: dict) -> tuple[bool, str]:
        tool      = decision.get("tool")
        arguments = decision.get("arguments", {})

        if not tool:
            return False, "No tool specified in decision."

        if tool not in TOOL_REGISTRY:
            return False, f"Unknown tool: '{tool}'. Not in registry."

        if tool in DESTRUCTIVE_TOOLS:
            return False, (
                f"Tool '{tool}' is destructive and requires explicit human approval."
            )

        tool_def      = TOOL_REGISTRY[tool]
        required_args = tool_def.get("required_args", [])
        for arg in required_args:
            if arg not in arguments:
                return False, f"Tool '{tool}' missing required argument: '{arg}'."

        valid, msg = self._validate_args(tool, arguments, tool_def)
        if not valid:
            return False, msg

        return True, "OK"

    def _validate_args(self, tool: str, arguments: dict, tool_def: dict) -> tuple[bool, str]:
        allowed = tool_def.get("allowed_args", {})
        for arg, value in arguments.items():
            if arg in allowed:
                allowed_values = allowed[arg]
                # FIX: normalize to str on both sides — LLM sends int 1,
                # allowed_args has int 1, but somewhere in JSON round-trip
                # they became mismatched causing BLOCKED on replication_factor=1
                normalized_value   = str(value)
                normalized_allowed = [str(v) for v in allowed_values]
                if normalized_value not in normalized_allowed:
                    return False, (
                        f"Argument '{arg}={value}' not allowed for tool '{tool}'. "
                        f"Allowed: {allowed_values}"
                    )
        return True, "OK"
