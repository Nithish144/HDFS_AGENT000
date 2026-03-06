"""
LLM Reasoner — Sends current state + gaps to OpenRouter and gets a tool decision.
"""
import json
import logging
import requests
from config.settings import Settings
from config.prompts import SYSTEM_PROMPT

logger           = logging.getLogger(__name__)
OPENROUTER_URL   = "https://openrouter.ai/api/v1/chat/completions"


class LLMReasoner:
    def __init__(self, settings: Settings):
        self.api_key = settings.openrouter_api_key
        self.model   = settings.llm_model
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type":  "application/json",
            "HTTP-Referer":  "https://github.com/hadoop-ai-agent",
            "X-Title":       "Hadoop AI Agent",
        }

    def decide(self, current_state: dict, gaps: list[dict],
               override_instruction: str = None) -> dict | None:
        user_message = self._build_user_message(current_state, gaps, override_instruction)
        payload = {
            "model":       self.model,
            "messages":    [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_message},
            ],
            "max_tokens":  512,
            "temperature": 0.1,
        }
        try:
            response = requests.post(OPENROUTER_URL, headers=self.headers,
                                     json=payload, timeout=30)
            response.raise_for_status()
            raw = response.json()["choices"][0]["message"]["content"].strip()
            logger.debug(f"LLM raw: {raw}")
            if raw.startswith("```"):
                parts = raw.split("```")
                raw   = parts[1] if len(parts) > 1 else raw
                if raw.startswith("json"):
                    raw = raw[4:]
            return json.loads(raw.strip())
        except requests.exceptions.HTTPError as e:
            logger.error(f"OpenRouter HTTP error: {e.response.status_code} — {e.response.text}")
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter request failed: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"LLM returned invalid JSON: {e}")
        return None

    def _build_user_message(self, current_state: dict, gaps: list[dict],
                            override_instruction: str = None) -> str:
        override_block = ""
        if override_instruction:
            override_block = (
                f"⚠️  AGENT OVERRIDE — FOLLOW THIS BEFORE ANYTHING ELSE:\n"
                f"{override_instruction}\n\n"
            )
        return (
            f"{override_block}"
            f"CURRENT CLUSTER STATE:\n{json.dumps(current_state, indent=2)}\n\n"
            f"GAPS (not yet meeting goal):\n{json.dumps(gaps, indent=2)}\n\n"
            f"Decide the single best tool call to make progress toward the goal state.\n"
            f"Respond ONLY with valid JSON in the required format."
        )
