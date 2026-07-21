# route_matcher.py
import re
import logging

logger = logging.getLogger(__name__)


class AsteriskPatternMatcher:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AsteriskPatternMatcher, cls).__new__(cls)
            cls._instance._cache = {}
        return cls._instance

    def asterisk_to_regex(self, pattern: str) -> str:
        """
        Convierte patrones de dialplan (ej: _9NXX.) a Regex de Python.
        """
        if pattern.startswith('_'):
            pattern = pattern[1:]

        regex = ""
        i = 0
        while i < len(pattern):
            char = pattern[i]
            if char == 'X':
                regex += r'[0-9]'
            elif char == 'Z':
                regex += r'[1-9]'
            elif char == 'N':
                regex += r'[2-9]'
            elif char == '.':
                regex += r'.+'
            elif char == '!':
                regex += r'.*'
            elif char == '[':
                end_bracket = pattern.find(']', i)
                if end_bracket != -1:
                    regex += pattern[i:end_bracket + 1]
                    i = end_bracket
                else:
                    regex += r'\['
            else:
                regex += re.escape(char)
            i += 1

        return f"^{regex}$"

    def match(self, number: str, patterns: list) -> bool:
        """
        Devuelve True si 'number' coincide con alguno de los 'patterns'.
        """
        for pat in patterns:
            if not pat:
                continue

            if pat not in self._cache:
                try:
                    regex_str = self.asterisk_to_regex(pat)
                    self._cache[pat] = re.compile(regex_str)
                except re.error as e:
                    logger.error(f"Patrón inválido '{pat}': {e}")
                    continue

            if self._cache[pat].match(str(number)):
                return True

        return False
