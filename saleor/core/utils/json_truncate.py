from dataclasses import InitVar, dataclass
from json.encoder import ESCAPE_ASCII, ESCAPE_DCT  # type: ignore


@dataclass
class JsonTruncText:
    text: str = ""
    truncated: bool = False
    added_bytes: InitVar[int] = 0
    ensure_ascii: InitVar[bool] = True

    def __post_init__(self, added_bytes, ensure_ascii):
        self._added_bytes = added_bytes
        self._ensure_ascii = ensure_ascii

    @property
    def byte_size(self) -> int:
        return len(self.text) + self._added_bytes

    @staticmethod
    def json_char_len(char: str, ensure_ascii=True) -> int:
        try:
            return len(ESCAPE_DCT[char])
        except KeyError:
            if ensure_ascii:
                return 6 if ord(char) < 0x10000 else 12
            return len(char.encode())

    @classmethod
    def truncate(cls, s: str, limit: int, ensure_ascii=True):
        limit = max(limit, 0)
        s_init_len = len(s)
        s = s[:limit]
        added_bytes = 0

        for match in ESCAPE_ASCII.finditer(s):
            start, end = match.span(0)
            markup = cls.json_char_len(match.group(0), ensure_ascii) - 1
            added_bytes += markup
            if end + added_bytes > limit:
                return cls(
                    text=s[:start],
                    truncated=True,
                    added_bytes=added_bytes - markup,
                    ensure_ascii=ensure_ascii,
                )
            elif end + added_bytes == limit:
                s = s[:end]
                return cls(
                    text=s,
                    truncated=len(s) < s_init_len,
                    added_bytes=added_bytes,
                    ensure_ascii=ensure_ascii,
                )
        return cls(
            text=s,
            truncated=len(s) < s_init_len,
            added_bytes=added_bytes,
            ensure_ascii=ensure_ascii,
        )
