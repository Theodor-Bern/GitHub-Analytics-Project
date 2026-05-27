#!/usr/bin/env python3

import time
import requests

API_VERSION = "2022-11-28"

TRANSIENT_STATUSES = {500, 502, 503, 504}
RETRY_ATTEMPTS = 5
RETRY_BASE_SECONDS = 2


class TokenPool:
    def __init__(self, tokens, low_water=25):
        # Dedup so the same PAT used in multiple slots collapses to one entry —
        # otherwise the pool double-counts its shared rate-limit budget.
        seen = set()
        self.tokens = []
        for t in tokens:
            t = (t or "").strip()
            if t and t not in seen:
                seen.add(t)
                self.tokens.append(t)
        if not self.tokens:
            raise RuntimeError("TokenPool: no GitHub tokens provided")
        self.low_water = low_water
        # remaining=None means "not used yet, assume full quota"
        self.remaining = {t: None for t in self.tokens}
        self.reset_at = {t: 0.0 for t in self.tokens}
        self.idx = 0

    def _auth_headers(self):
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.tokens[self.idx]}",
            "X-GitHub-Api-Version": API_VERSION,
        }

    def _record(self, resp):
        token = self.tokens[self.idx]
        rem = resp.headers.get("X-RateLimit-Remaining")
        rst = resp.headers.get("X-RateLimit-Reset")
        if rem is not None:
            self.remaining[token] = int(rem)
        if rst is not None:
            self.reset_at[token] = float(rst)

    def _usable(self, token):
        rem = self.remaining[token]
        if rem is None or rem > self.low_water:
            return True
        return time.time() >= self.reset_at[token]

    def _ensure_capacity(self):
        if self._usable(self.tokens[self.idx]):
            return
        for i, token in enumerate(self.tokens):
            if self._usable(token):
                self.idx = i
                print(f"  [token-pool] rotated to token #{i + 1}", flush=True)
                return
        # All tokens spent — wait for the soonest reset and treat them as fresh.
        wait = max(0, min(self.reset_at.values()) - time.time()) + 2
        print(f"  [token-pool] all {len(self.tokens)} tokens spent, "
              f"sleeping {wait:.0f}s", flush=True)
        time.sleep(wait)
        for token in self.tokens:
            self.remaining[token] = None
        self.idx = 0

    def _get_once(self, url, **kwargs):
        """One GET that rotates on rate-limit 403 and retries 5xx with backoff."""
        resp = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            for _ in range(len(self.tokens) + 1):
                self._ensure_capacity()
                resp = requests.get(url, headers=self._auth_headers(), **kwargs)
                self._record(resp)
                if resp.status_code == 403 and "rate limit" in resp.text.lower():
                    self.remaining[self.tokens[self.idx]] = 0
                    continue
                break

            if resp.status_code not in TRANSIENT_STATUSES:
                return resp

            if attempt == RETRY_ATTEMPTS:
                return resp

            wait = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            print(
                f"  [token-pool] HTTP {resp.status_code} on {url}, "
                f"attempt {attempt}/{RETRY_ATTEMPTS}, sleeping {wait}s",
                flush=True,
            )
            time.sleep(wait)
        return resp

    def get(self, url, **kwargs):
        return self._get_once(url, **kwargs)
