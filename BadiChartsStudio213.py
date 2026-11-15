#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import io
import ssl
import json

# ==============================================================================
# ENCODING FIX: Force UTF-8 on stdout / stderr / stdin to prevent
# UnicodeEncodeError / UnicodeDecodeError when the script prints or logs
# non-ASCII characters (Arabic, accents, emoji, etc.) on Windows consoles
# whose default code page is cp1252 / cp1256.
# ==============================================================================
# Fix console encoding on Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
        sys.stdin.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Set default encoding to UTF-8
try:
    io.TextIOWrapper(open.__self__, encoding='utf-8')
except Exception:
    pass
import time
import math
import struct
import logging
import asyncio
import calendar
import platform
import random
import re
import shutil
import threading
import traceback
import itertools
import configparser
import contextlib
from pathlib import Path
from datetime import datetime, timedelta
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Awaitable, Callable, Dict, Generic, Hashable, List, Literal, Mapping, Optional, Tuple, TypeVar

import certifi
import requests
import websocket
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from bs4.element import AttributeValueList
from fake_useragent import UserAgent

try:
    import orjson as _orjson
    HAS_ORJSON = True
except ImportError:
    _orjson = None
    HAS_ORJSON = False

# ==============================================================================
# FIX FOR NUITKA / FROZEN ENVIRONMENT
# ==============================================================================
# 1) fake_useragent: force local-only mode so it doesn't try to fetch a remote
#    user-agent list (which would crash when running as a frozen EXE / Nuitka
#    build with no network access or no writable cache dir).
os.environ['HTTPX_FAKE_USERAGENT_LOCAL_ONLY'] = 'True'
os.environ['FAKE_USERAGENT_FALLBACK'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# 2) SSL certificates: pin certifi's CA bundle path into the env vars that
#    requests / websocket-client / ssl module all read. This avoids
#    "SSL: CERTIFICATE_VERIFY_FAILED" errors in frozen builds where the
#    default cert store is missing or points to a temp dir.
_cert_path = certifi.where()
os.environ['SSL_CERT_FILE'] = _cert_path
os.environ['WEBSOCKET_CLIENT_CA_BUNDLE'] = _cert_path
os.environ['REQUESTS_CA_BUNDLE'] = _cert_path

# 3) Working directory: chdir to the folder that contains the EXE (frozen) or
#    the .py file (script mode) so app_data.db, mt4_config.json, etc. are
#    created next to the binary — NOT inside a temp _MEIPASS / build dir.
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    os.chdir(BASE_DIR)
except Exception:
    pass

# ==============================================================================
# SECTION 1: LOGGING & SSL SETUP
# ==============================================================================
def _prepare_logging():
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    websocket_logger = logging.getLogger("websocket")
    websocket_logger.setLevel(logging.INFO)
    websocket_logger.addHandler(logging.NullHandler())

_prepare_logging()
logger = logging.getLogger(__name__)

cert_path = certifi.where()
os.environ['SSL_CERT_FILE'] = cert_path
os.environ['WEBSOCKET_CLIENT_CA_BUNDLE'] = cert_path
cacert = os.environ.get('WEBSOCKET_CLIENT_CA_BUNDLE')

ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
ssl_context.load_verify_locations(cert_path)


# SECTION 1.6: STARTUP WELCOME (بدون ترخيص — نظام الترخيص أُزيل بالكامل)
def qx_print_welcome():
    """شعار ترحيبي بسيط — لا تحقق ترخيص ولا اتصال خارجي."""
    print()
    print(f"\033[96m{'═' * 62}\033[0m")
    print(f"\033[97m\033[1m  QX Console — Quotex Candles & Assets\033[0m\033[0m")
    print(f"\033[2m  v14 · default asset: BRLUSD_otc · SQLite database · every-tick · stable-after-fill\033[0m")
    print(f"\033[96m{'═' * 62}\033[0m")
    print()
# ==============================================================================
# SECTION 1.5: EVENT-DRIVEN WAIT PRIMITIVES (replaces asyncio.sleep polling)
# ==============================================================================
# pyquotex._api._waits — " " .
# polling timeout asyncio.sleep
# asyncio.Event .

async def wait_until(predicate: Callable[[], bool], *, timeout: float = 10.0,
                      poll_interval: float = 0.05) -> None:
    """ predicate() poll_interval True timeout.

 asyncio.TimeoutError . :
 while not condition:
 await asyncio.sleep(X)
 latency X poll_interval (50ms ).
 """
    async def _loop():
        while not predicate():
            await asyncio.sleep(poll_interval)
    await asyncio.wait_for(_loop(), timeout=timeout)


async def wait_for_first_event(*events: asyncio.Event, timeout: float = 10.0) -> int:
    """ event index. TimeoutError .

 :
 for _ in range(100):
 if cond_a: return A
 if cond_b: return B
 await asyncio.sleep(0.1)
 """
    if not events:
        raise ValueError("wait_for_first_event requires at least one event")
    tasks = [asyncio.ensure_future(e.wait()) for e in events]
    try:
        done, pending = await asyncio.wait(tasks, timeout=timeout,
                                           return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for i, t in enumerate(tasks):
            if t in done and t.result() and not t.cancelled():
                return i
        # timeout
        raise asyncio.TimeoutError()
    except asyncio.CancelledError:
        for t in tasks:
            if not t.done():
                t.cancel()
        raise


def _schedule_event_set(event: Optional[asyncio.Event],
                        loop: Optional[asyncio.AbstractEventLoop]) -> None:
    """ asyncio.Event thread ( websocket thread).

 run_coroutine_threadsafe coroutine 
 callback loop thread-safe loop.call_soon_threadsafe.
 """
    if event is None or loop is None:
        return
    try:
        if not loop.is_closed() and loop.is_running():
            loop.call_soon_threadsafe(event.set)
    except RuntimeError:
        # loop —
        pass


# ==============================================================================
# SECTION 2: CONSTANTS & SESSION
# ==============================================================================
USER_AGENT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
base_dir = Path.cwd()
session_lock = threading.Lock()

def resource_path(relative_path: str) -> Path:
    global base_dir
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        base_dir = Path(sys._MEIPASS)
    return base_dir / relative_path

# ----------------------------------------------------------------------
# SECTION 2B: DATABASE STORAGE
# Replaces saved_email.txt, session.json, session_state.json with a single
# SQLite database file (app_data.db). SQLite is built into Python, so there
# are no extra dependencies. All credentials, session cookies/tokens and
# login state now live in two tables:
#   - sessions  (email -> JSON blob with cookies/token/user_agent)
#   - app_state (single-row table: email, password, host, last_login, ...)
# On every startup, the sessions table is wiped so the app performs a fresh
# login (no stale cookies/token) — see SessionManager.delete_session_file().
# ----------------------------------------------------------------------
import sqlite3 as _sqlite3

DB_PATH = Path(resource_path("app_data.db"))
_db_lock = threading.Lock()

def _db_conn() -> "_sqlite3.Connection":
    conn = _sqlite3.connect(str(DB_PATH), timeout=15, check_same_thread=False)
    conn.row_factory = _sqlite3.Row
    return conn

def _init_db() -> None:
    try:
        DB_PATH.parent.mkdir(exist_ok=True, parents=True)
    except Exception:
        pass
    with _db_lock:
        conn = _db_conn()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    email TEXT PRIMARY KEY,
                    data  TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS app_state (
                    id                  INTEGER PRIMARY KEY CHECK (id = 1),
                    email               TEXT,
                    password            TEXT,
                    host                TEXT,
                    last_login          REAL,
                    last_success        INTEGER,
                    failed_auth_count   INTEGER
                )
            """)
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()


_init_db()



def load_session(email: str, user_agent: str = None) -> dict:
    if user_agent is None:
        try: user_agent = UserAgent().random
        except Exception: user_agent = USER_AGENT
    default_data = {"cookies": None, "token": None, "user_agent": user_agent}
    with _db_lock:
        conn = _db_conn()
        try:
            row = conn.execute("SELECT data FROM sessions WHERE email=?", (email,)).fetchone()
            if row:
                try:
                    data = json.loads(row["data"])
                    if not data.get("user_agent"):
                        data["user_agent"] = user_agent
                    return data
                except json.JSONDecodeError:
                    pass
            conn.execute(
                "INSERT OR REPLACE INTO sessions (email, data) VALUES (?, ?)",
                (email, json.dumps(default_data, indent=2))
            )
            conn.commit()
            return default_data
        except Exception:
            return default_data
        finally:
            conn.close()

def update_session(email: str, d: dict) -> dict:
    with _db_lock:
        conn = _db_conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO sessions (email, data) VALUES (?, ?)",
                (email, json.dumps(d, indent=2))
            )
            conn.commit()
            return d
        except Exception:
            return d
        finally:
            conn.close()

# ==============================================================================
# SECTION 3: HTTP NAVIGATOR (PROVEN WORKING METHOD)
# ==============================================================================
retry_strategy = Retry(
    total=3, backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504, 104],
    allowed_methods=["HEAD", "POST", "PUT", "GET", "OPTIONS"],
)

class CipherSuiteAdapter(HTTPAdapter):
    __attrs__ = ['ssl_context', 'max_retries', 'config', '_pool_connections', '_pool_maxsize', '_pool_block', 'source_address']
    def __init__(self, *args, **kwargs):
        self.ssl_context = kwargs.pop('ssl_context', None)
        self.cipherSuite = kwargs.pop('cipherSuite', 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384')
        self.source_address = kwargs.pop('source_address', None)
        self.server_hostname = kwargs.pop('server_hostname', None)
        self.ecdhCurve = kwargs.pop('ecdhCurve', 'prime256v1')
        if not self.ssl_context:
            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            self.ssl_context.orig_wrap_socket = self.ssl_context.wrap_socket
            self.ssl_context.wrap_socket = self.wrap_socket
        if self.server_hostname:
            self.ssl_context.server_hostname = self.server_hostname
        if self.cipherSuite:
            self.ssl_context.set_ciphers(self.cipherSuite)
            self.ssl_context.set_ecdh_curve(self.ecdhCurve)
            self.ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            self.ssl_context.maximum_version = ssl.TLSVersion.TLSv1_3
        super().__init__(**kwargs)

    def wrap_socket(self, *args, **kwargs):
        if hasattr(self.ssl_context, 'server_hostname') and self.ssl_context.server_hostname:
            kwargs['server_hostname'] = self.ssl_context.server_hostname
            self.ssl_context.check_hostname = False
        else:
            self.ssl_context.check_hostname = True
        return self.ssl_context.orig_wrap_socket(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self.ssl_context
        kwargs['source_address'] = self.source_address
        return super().init_poolmanager(*args, **kwargs)

class Browser(Session):
    def __init__(self, *args, **kwargs):
        self.response = None
        self.default_headers = None
        self.ecdhCurve = kwargs.pop('ecdhCurve', 'prime256v1')
        self.cipherSuite = kwargs.pop('cipherSuite', 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384')
        self.source_address = kwargs.pop('source_address', None)
        self.server_hostname = kwargs.pop('server_hostname', None)
        self.proxies = kwargs.pop('proxies', None)
        super().__init__(*args, **kwargs)
        self.headers.update(self.get_headers())
        self.mount('https://', CipherSuiteAdapter(ecdhCurve=self.ecdhCurve, cipherSuite=self.cipherSuite, server_hostname=self.server_hostname, source_address=self.source_address, ssl_context=ssl_context, max_retries=retry_strategy))

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.close()
    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc_val, exc_tb): self.__exit__(exc_type, exc_val, exc_tb)

    def get_headers(self):
        self.default_headers = {"User-Agent": USER_AGENT}
        return self.default_headers

    def set_headers(self, headers=None):
        self.headers.update(self.default_headers)
        if headers: self.headers.update(headers)

    def get_cookies(self):
        return '; '.join(f'{i.name}={i.value}' for i in self.cookies)

    def get_soup(self):
        if self.response and not self.response.ok: raise RuntimeError(self.response.reason)
        # Use raw bytes so BeautifulSoup can detect the real encoding;
        # never rely on requests' .text which may raise UnicodeDecodeError.
        try:
            raw = self.response.content if self.response is not None else b""
            return BeautifulSoup(raw, "html.parser", from_encoding="utf-8")
        except Exception:
            return BeautifulSoup(b"", "html.parser")

    def send_request(self, method, url, headers=None, **kwargs):
        merged_headers = self.headers.copy()
        if headers: merged_headers.update(headers)
        if self.proxies: kwargs['proxies'] = self.proxies
        self.response = self.request(method, url, headers=merged_headers, **kwargs)
        return self.response

# ==============================================================================
# SECTION 4: HTTP LOGIN & SETTINGS
# ==============================================================================
class Login(Browser):
    url = ""
    cookies = None
    ssid = None
    # FALLBACK default only - real value is set per-instance from api.host
    base_url = 'qxbroker.com'
    https_base_url = f'https://{base_url}'

    def __init__(self, api, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = api
        # ---- DYNAMIC DOMAIN: follow the user-selected host -----------------
        # All HTTP login traffic now goes to the SAME domain that the WebSocket
        # will later connect to via ws2.<host>, so cookies/token stay valid.
        if not getattr(api, 'host', None):
            pass  # keep the hard-coded fallback
        else:
            self.base_url = api.host
            self.https_base_url = f'https://{self.base_url}'
        self.headers = self.get_headers()
        self.full_url = f"{self.https_base_url}/{api.lang}"

    def get_token(self):
        self.headers["Connection"] = "keep-alive"
        self.headers["Accept-Encoding"] = "gzip, deflate, br"
        self.headers["Accept-Language"] = "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3"
        self.headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        self.headers["Referer"] = f"{self.full_url}/sign-in"
        self.headers["Upgrade-Insecure-Requests"] = "1"
        self.headers["Sec-Ch-Ua-Mobile"] = "?0"
        self.headers["Sec-Ch-Ua-Platform"] = '"Linux"'
        self.headers["Sec-Fetch-Site"] = "same-origin"
        self.headers["Sec-Fetch-User"] = "?1"
        self.headers["Sec-Fetch-Dest"] = "document"
        self.headers["Sec-Fetch-Mode"] = "navigate"
        self.headers["Dnt"] = "1"
        self.send_request("GET", f"{self.full_url}/sign-in/modal/")
        html = self.get_soup()
        match = html.find("input", {"name": "_token"})
        return None if not match else match.get("value")

    async def awaiting_pin(self, data, input_message):
        self.headers["Content-Type"] = "application/x-www-form-urlencoded"
        self.headers["Referer"] = f"{self.full_url}/sign-in/modal"
        data["keep_code"] = 1
        try:
            code = input(input_message)
            if not code.isdigit():
                print("Please enter a valid code.")
                await self.awaiting_pin(data, input_message)
            data["code"] = code
        except KeyboardInterrupt:
            print("\nClosing program.")
            sys.exit()
        await asyncio.sleep(1)
        self.send_request(method="POST", url=f"{self.full_url}/sign-in/modal", data=data)

    def get_profile(self):
        self.response = self.send_request(method="GET", url=f"{self.full_url}/trade")
        if self.response:
            script = self.get_soup().find_all("script", {"type": "text/javascript"})
            script = script[0].get_text() if script else "{}"
            match = script.strip().replace(";", "").replace("window.settings = ", "")
            self.cookies = self.get_cookies()
            try:
                settings_dict = json.loads(match)
                self.ssid = settings_dict.get("token")
            except json.JSONDecodeError:
                self.ssid = None
            self.api.session_data["cookies"] = self.cookies
            self.api.session_data["token"] = self.ssid
            self.api.session_data["user_agent"] = self.headers["User-Agent"]
            update_session(self.api.username, self.api.session_data)
            return self.response, settings_dict if self.ssid else None
        return None, None

    async def _post(self, data):
        self.response = self.send_request(method="POST", url=f"{self.full_url}/sign-in/", data=data)
        required_keep_code = self.get_soup().find("input", {"name": "keep_code"})
        if required_keep_code:
            auth_body = self.get_soup().find("main", {"class": "auth__body"})
            input_message = f'{auth_body.find("p").text}: ' if auth_body.find("p") else "Enter the PIN code sent to your email: "
            await self.awaiting_pin(data, input_message)
            await asyncio.sleep(1)
            return self.success_login()
        return self.success_login()

    def success_login(self):
        if "trade" in str(self.response.url):
            return True, "Login successful."
        soup = self.get_soup()
        not_available = soup.select_one("#tab-1 > div > div.modal-sign__not-avalible__title")
        if not_available: return False, f"Service unavailable: {not_available.get_text(strip=True)}"
        error = soup.select_one("#tab-1 form > div:nth-child(2) > div")
        msg = error.get_text(strip=True) if error else "Unknown error"
        return False, f"Login failed. {msg}"

    async def __call__(self, username, password, user_data_dir=None):
        data = {"_token": self.get_token(), "email": username, "password": password, "remember": 1}
        status, msg = await self._post(data)
        if status: self.get_profile()
        return status, msg

class Settings(Browser):
    def __init__(self, api):
        super().__init__()
        self.set_headers()
        self.api = api
        self.headers = self.get_headers()

    def get_settings(self):
        self.headers["content-type"] = "application/json"
        self.headers["referer"] = f"{self.api.https_url}/{self.api.lang}/trade"
        self.headers["cookie"] = self.api.session_data.get("cookies", "")
        self.headers["user-agent"] = self.api.session_data.get("user_agent", "")
        response = self.send_request("GET", f"{self.api.https_url}/api/v1/cabinets/digest")
        return response.json()

# ==============================================================================
# SECTION 5: WEBSOCKET CLIENT & STATE (SYNC + EVENT REGISTRY INTEGRATION)
# ==============================================================================
class WebsocketStatus(IntEnum):
    DISCONNECTED = 0
    CONNECTED = 1
    CONNECTING = 2
    ERROR = -1

class AuthStatus(IntEnum):
    NOT_AUTHENTICATED = 0
    AUTHENTICATING = 1
    AUTHENTICATED = 2
    FAILED = -1

class ConnectionState:
    def __init__(self):
        self.SSID = None
        self.status = WebsocketStatus.DISCONNECTED
        self.auth_status = AuthStatus.NOT_AUTHENTICATED
        self.ssl_Mutual_exclusion = False
        self.ssl_Mutual_exclusion_write = False
        self.check_rejected_connection = False
        self.check_accepted_connection = False
        self.check_websocket_if_error = False
        self.check_websocket_if_connect = None
        self.websocket_error_reason = None

        # ===== EVENT-DRIVEN ADDITIONS =====
        # asyncio polling. lazily async context
        # init_events() "no running event loop" .
        self.ws_connected_event: Optional[asyncio.Event] = None
        self.ws_closed_event: Optional[asyncio.Event] = None
        self.auth_accepted_event: Optional[asyncio.Event] = None
        self.auth_rejected_event: Optional[asyncio.Event] = None
        self.ws_error_event: Optional[asyncio.Event] = None
        # loop websocket thread
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def init_events(self) -> None:
        """ loop . async context."""
        if self.ws_connected_event is None:
            self.ws_connected_event = asyncio.Event()
        if self.ws_closed_event is None:
            self.ws_closed_event = asyncio.Event()
        if self.auth_accepted_event is None:
            self.auth_accepted_event = asyncio.Event()
        if self.auth_rejected_event is None:
            self.auth_rejected_event = asyncio.Event()
        if self.ws_error_event is None:
            self.ws_error_event = asyncio.Event()
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

    def reset_events(self) -> None:
        """ ."""
        for ev in (self.ws_connected_event, self.ws_closed_event,
                   self.auth_accepted_event, self.auth_rejected_event,
                   self.ws_error_event):
            if ev is not None:
                ev.clear()

    # wrappers thread (websocket thread)
    def signal_ws_connected(self) -> None:
        _schedule_event_set(self.ws_connected_event, self._loop)

    def signal_ws_closed(self) -> None:
        _schedule_event_set(self.ws_closed_event, self._loop)

    def signal_auth_accepted(self) -> None:
        _schedule_event_set(self.auth_accepted_event, self._loop)

    def signal_auth_rejected(self) -> None:
        _schedule_event_set(self.auth_rejected_event, self._loop)

    def signal_ws_error(self) -> None:
        _schedule_event_set(self.ws_error_event, self._loop)

class WebsocketClient:
    def __init__(self, api):
        self.api = api
        self.state = api.state
        self.headers = {
            "User-Agent": self.api.session_data.get("user_agent", USER_AGENT),
            "Origin": self.api.https_url,
            "Host": f"ws2.{self.api.host}",
        }
        self.wss = websocket.WebSocketApp(
            self.api.wss_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
            on_ping=self.on_ping,
            on_pong=self.on_pong,
            header=self.headers,
            cookie=self.api.session_data.get("cookies"),
        )

    def on_message(self, wss, msg):
        self.state.ssl_Mutual_exclusion = True
        # ===== STALE-WATCHDOG: =====
        # watchdog .
        try:
            if self.api is not None:
                self.api.last_message_at = time.time()
        except Exception:
            pass
        try:
            # Defensive: a malformed binary frame must never crash the WS thread.
            if isinstance(msg, (bytes, bytearray, memoryview)):
                msg_str = bytes(msg).decode("utf-8", errors="ignore")
            else:
                msg_str = str(msg)

            if msg_str == "2":
                try:
                    self.wss.send("3")
                except Exception:
                    pass
                self.state.ssl_Mutual_exclusion = False
                return
            if msg_str == "3":
                self.state.ssl_Mutual_exclusion = False
                return

            if "authorization/reject" in msg_str:
                logger.warning("Token rejected.")
                self.state.check_rejected_connection = True
                self.state.auth_status = AuthStatus.FAILED
                # ===== EVENT-DRIVEN FIX =====
                self.state.signal_auth_rejected()
            elif "s_authorization" in msg_str:
                self.state.check_accepted_connection = True
                self.state.check_rejected_connection = False
                self.state.auth_status = AuthStatus.AUTHENTICATED
                self.state.status = WebsocketStatus.CONNECTED
                print("\n" + "="*60)
                print("Connected to server successfully")
                print("="*60 + "\n")
                # ===== EVENT-DRIVEN FIX =====
                self.state.signal_auth_accepted()

            # ===== INSTRUMENTS LIST (طريقة pyquotex الرسمية — من الكود الأول) =====
            # الرد على 42["instruments/get"] يصل بصيغة 451-["instruments/list",[...]]
            # — نلتقطه، نخزنه في self.api.instruments ونوقظ كل من ينتظر instruments_ready.
            if "instruments/list" in msg_str:
                try:
                    _idx = msg_str.find('["')
                    if _idx != -1:
                        _payload = json.loads(msg_str[_idx:])
                        _data = _payload[1] if isinstance(_payload, list) and len(_payload) >= 2 else None
                        if isinstance(_data, dict) and "list" in _data:
                            _data = _data["list"]
                        if isinstance(_data, list) and _data:
                            self.api.instruments = _data
                            _loop = getattr(self.api, "_async_loop", None)
                            if _loop is not None and _loop.is_running():
                                asyncio.run_coroutine_threadsafe(
                                    self.api.event_registry.set_event("instruments_ready", _data),
                                    _loop,
                                )
                            logger.info(f"Instruments list received: {len(_data)} assets")
                except Exception as _e:
                    logger.debug(f"instruments/list parse error: {_e}")

            message = None
            if len(msg_str) > 1 and msg_str[1] in ('[', '{'):
                try:
                    message = json.loads(msg_str[1:])
                except Exception:
                    pass

            if message is not None:
                self._process_message_and_raise_events(message)

            if str(msg_str) == "41":
                self.state.check_websocket_if_connect = 0
        except Exception as e:
            logger.error("Unhandled error in on_message: %s", e)
        self.state.ssl_Mutual_exclusion = False

    def _process_message_and_raise_events(self, message):
        try:
            loop = self.api._async_loop
            if loop is None or not loop.is_running():
                return

            if isinstance(message, dict):
                asset = message.get("asset")
                if asset and (message.get("candles") or message.get("data") or message.get("history")):
                    self.api.candle_v2_data[asset] = message
                    self.api.candles.candles_data = message.get("candles") or message.get("data") or message.get("history")

                    asyncio.run_coroutine_threadsafe(
                        self.api.event_registry.set_event(f'candles_ready_{asset}', message),
                        loop
                    )

                    index = message.get("index")
                    if index is not None:
                        asyncio.run_coroutine_threadsafe(
                            self.api.event_registry.set_event(f'candles_ready_{asset}_{index}', message),
                            loop
                        )

            if isinstance(message, dict) and (message.get("liveBalance") or message.get("demoBalance")):
                self.api.account_balance = message

            if isinstance(message, list) and len(message) > 0 and isinstance(message[0], list) and len(message[0]) == 4:
                asset = message[0][0]
                self.api.realtime_candles[asset] = message[0]
        except Exception as e:
            logger.debug(f"Error processing message: {e}")

    def on_error(self, wss, error):
        global CONNECTION_ALIVE
        logger.error(error)
        self.state.websocket_error_reason = str(error)
        self.state.check_websocket_if_error = True
        self.state.status = WebsocketStatus.ERROR
        self.state.check_accepted_connection = False
        # ===== EVENT-DRIVEN FIX =====
        self.state.signal_ws_error()
        try:
            CONNECTION_ALIVE = False
        except Exception:
            pass

    def on_open(self, wss):
        logger.info("Websocket client connected.")
        self.state.check_websocket_if_connect = 1
        self.state.status = WebsocketStatus.CONNECTED
        # ===== EVENT-DRIVEN FIX =====
        self.state.signal_ws_connected()
        asset_name = self.api.current_asset or "EURUSD"
        period = self.api.current_period or 60
        self.wss.send('42["tick"]')
        self.wss.send('42["indicator/list"]')
        self.wss.send('42["drawing/load"]')
        self.wss.send('42["pending/list"]')
        self.wss.send(f'42["instruments/update",{{"asset":"{asset_name}","period":{period}}}]')
        self.wss.send(f'42["depth/follow","{asset_name}"]')
        self.wss.send('42["chart_notification/get"]')
        self.wss.send('42["instruments/get"]')
        self.wss.send('42["tick"]')

    def on_close(self, wss, close_status_code, close_msg):
        global CONNECTION_ALIVE
        logger.info("Websocket connection closed.")
        self.state.check_websocket_if_connect = 0
        self.state.status = WebsocketStatus.DISCONNECTED
        self.state.check_accepted_connection = False
        # ===== EVENT-DRIVEN FIX =====
        self.state.signal_ws_closed()
        try:
            CONNECTION_ALIVE = False
        except Exception:
            pass

    def on_ping(self, wss, ping_msg): pass
    def on_pong(self, wss, pong_msg): pass

# ==============================================================================
# SECTION 6: QUOTEX API CORE
# ==============================================================================
class CandlesObj:
    def __init__(self): self.__candles_data = None
    @property
    def candles_data(self): return self.__candles_data
    @candles_data.setter
    def candles_data(self, candles_data): self.__candles_data = candles_data

class EventRegistry:
    def __init__(self):
        self._events: Dict[str, asyncio.Event] = {}
        self._data: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def get_event(self, key: str) -> asyncio.Event:
        async with self._lock:
            if key not in self._events:
                self._events[key] = asyncio.Event()
            return self._events[key]

    async def set_event(self, key: str, data: Any = None):
        async with self._lock:
            if key not in self._events:
                self._events[key] = asyncio.Event()
            self._data[key] = data
            self._events[key].set()

    async def wait_event(self, key: str, timeout: float = 30.0) -> Any:
        event = await self.get_event(key)
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return self._data.get(key)
        except asyncio.TimeoutError:
            return None

    async def clear_event(self, key: str):
        async with self._lock:
            if key in self._events:
                self._events[key].clear()
            if key in self._data:
                del self._data[key]

class QuotexAPI:
    def __init__(self, host, username, password, lang, proxies=None, user_data_dir="."):
        self.state = ConnectionState()
        self.trace_ws = False
        self.current_asset = None
        self.current_period = None
        self.account_balance = None
        self.account_type = 1
        self.instruments = None
        self.host = host
        self.https_url = f"https://{host}"
        self.wss_url = f"wss://ws2.{host}/socket.io/?EIO=3&transport=websocket"
        self.websocket_thread = None
        self.websocket_client = None
        self.username = username
        self.password = password
        self.proxies = proxies
        self.lang = lang
        self.user_data_dir = user_data_dir
        self.session_data = {}
        self.browser = Browser()
        self.browser.set_headers()
        self.settings = Settings(self)
        self.candles = CandlesObj()
        self.candle_v2_data = {}
        self.realtime_price = defaultdict(list)
        self.realtime_candles = {}
        self.event_registry = EventRegistry()
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._temp_status = ""
        # ===== STALE-WATCHDOG ADDITION =====
        # WS . watchdog
        # .
        self.last_message_at: float = time.time()
        # ===== FIX 7: Rate Limiter ( ) =====
        # . rate limiter
        # keepalive + watchdog.
        # rate limiter (15ms = ~66 req/s) bursts .
        self._ws_send_times: list[float] = []
        self._ws_min_interval: float = 0.015  # 15ms ( )
        self._ws_max_per_window: int = 100    # 100 / ()
        self._ws_window: float = 1.0          # 1
        self._throttle_multiplier: float = 1.0  # = 1.0 ( auto-throttle)
        self._last_send_at: float = 0.0

    @property
    def login(self): return Login(self)

    def _apply_rate_limit(self) -> None:
        """ rate limiting WS — .

 - (_ws_min_interval * _throttle_multiplier)
 - (_ws_max_per_window / _throttle_multiplier)
 """
        now = time.time()
        # 1)
        min_interval = self._ws_min_interval * self._throttle_multiplier
        if self._last_send_at > 0:
            elapsed = now - self._last_send_at
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
                now = time.time()
        # 2)
        window_size = self._ws_window * self._throttle_multiplier
        cutoff = now - window_size
        self._ws_send_times = [t for t in self._ws_send_times if t >= cutoff]
        # 3)
        if len(self._ws_send_times) >= self._ws_max_per_window:

            sleep_for = (self._ws_send_times[0] + window_size) - now
            if sleep_for > 0:
                time.sleep(sleep_for)
                now = time.time()
                cutoff = now - window_size
                self._ws_send_times = [t for t in self._ws_send_times if t >= cutoff]
        # 4)
        self._ws_send_times.append(now)
        self._last_send_at = now

    def increase_throttle(self, factor: float = 2.0, max_multiplier: float = 8.0) -> None:
        """ ( ).
 => => => .
 """
        new_multiplier = min(self._throttle_multiplier * factor, max_multiplier)
        if new_multiplier != self._throttle_multiplier:
            self._throttle_multiplier = new_multiplier
            logger.info(f" Throttle increased to {new_multiplier}x (rate limit stress)")

    def decrease_throttle(self, factor: float = 0.5, min_multiplier: float = 1.0) -> None:
        """ ( )."""
        new_multiplier = max(self._throttle_multiplier * factor, min_multiplier)
        if new_multiplier != self._throttle_multiplier:
            self._throttle_multiplier = new_multiplier
            logger.info(f" Throttle restored to {new_multiplier}x")

    def send_websocket_request(self, data, no_force_send=True):
        # ===== FIX: busy-wait -> sleep + try/finally =====
        if no_force_send:
            deadline = time.time() + 5.0  # 5s
            while (self.state.ssl_Mutual_exclusion or self.state.ssl_Mutual_exclusion_write):
                if time.time() > deadline:
                    # — . .
                    break
                time.sleep(0.001)  # `pass` — CPU
        # ===== FIX 6: rate limit =====
        # : heartbeat messages ("2" "42[\"tick\"]" "42[\"instruments/get\"]")
        # rate limit — keepalive.
        is_heartbeat = (
            data == "2" or
            data == '42["tick"]' or
            data == '42["instruments/get"]' or
            'pending/list' in data or
            'indicator/list' in data or
            'drawing/load' in data
        )
        if not is_heartbeat:
            try:
                self._apply_rate_limit()
            except Exception:
                # rate limiter
                pass
        self.state.ssl_Mutual_exclusion_write = True
        try:
            if self.websocket_client and self.websocket_client.wss:
                self.websocket_client.wss.send(data)
        finally:
            # ===== FIX: flag =====
            self.state.ssl_Mutual_exclusion_write = False

    def subscribe_realtime_candle(self, asset, period):
        self.realtime_price[asset] = []
        self.realtime_candles[asset] = {}
        data = f'42["instruments/update", {json.dumps({"asset": asset, "period": period})}]'
        return self.send_websocket_request(data)

    def follow_candle(self, asset):
        return self.send_websocket_request(f'42["depth/follow", {json.dumps(asset)}]')

    def chart_notification(self, asset):
        return self.send_websocket_request(f'42["chart_notification/get", {json.dumps({"asset": asset, "version": "1.0.0"})}]')

    def get_candles_ws(self, asset, index, time_val, offset, period):
        payload = {"asset": asset, "index": index, "time": time_val, "offset": offset, "period": period}
        data = f'42["history/load",{json.dumps(payload)}]'
        return self.send_websocket_request(data)

    async def authenticate(self):
        async with self.login as login:
            status, msg = await login(self.username, self.password, self.user_data_dir)
        if status:
            self.state.SSID = self.session_data.get("token")
        return status, msg

    async def start_websocket(self):
        self.state.check_websocket_if_connect = None
        self.state.check_websocket_if_error = False
        self.state.websocket_error_reason = None
        # ===== EVENT-DRIVEN FIX =====
        # +
        self.state.init_events()
        self.state.reset_events()
        # loop ( reconnect)
        try:
            self.state._loop = asyncio.get_running_loop()
        except RuntimeError:
            self.state._loop = None

        if not self.state.SSID:
            await self.authenticate()
        self.websocket_client = WebsocketClient(self)
        payload = {
            "suppress_origin": True, "ping_interval": 24, "ping_timeout": 20, "ping_payload": "2",
            "origin": self.https_url, "host": f"ws2.{self.host}",
            "sslopt": {"check_hostname": True, "cert_reqs": ssl.CERT_REQUIRED, "ca_certs": cacert, "context": ssl_context},
        }
        if platform.system() == "Linux":
            payload["sslopt"]["ssl_version"] = ssl.PROTOCOL_TLS
        self.websocket_thread = threading.Thread(target=self.websocket_client.wss.run_forever, kwargs=payload)
        self.websocket_thread.daemon = True
        self.websocket_thread.start()

        # ===== EVENT-DRIVEN FIX =====
        # polling:
        # for _ in range(100):
        # if self.state.check_websocket_if_error: return False, ...
        # elif self.state.check_websocket_if_connect == 1: return True, ...
        # elif self.state.check_rejected_connection: return False, "Token Rejected."
        # await asyncio.sleep(0.1)
        # : connected, rejected, error, closed
        try:
            idx = await wait_for_first_event(
                self.state.ws_connected_event,
                self.state.auth_rejected_event,
                self.state.ws_error_event,
                self.state.ws_closed_event,
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            return False, "Timeout waiting for websocket open"

        if idx == 0:
            return True, "Websocket connected successfully!!!"
        elif idx == 1:
            self.state.SSID = None
            return False, "Websocket Token Rejected."
        elif idx == 2:
            return False, self.state.websocket_error_reason or "Websocket error"
        elif idx == 3:
            return False, "Websocket connection closed."
        return False, "Unknown websocket state"

    async def send_ssid(self, timeout=10):
        if not self.state.SSID: return False
        # ===== EVENT-DRIVEN FIX =====
        # ( send_ssid start_websocket )
        if self.state.auth_accepted_event is None:
            self.state.init_events()
        self.state.auth_accepted_event.clear()
        self.state.auth_rejected_event.clear()

        payload = {"session": self.state.SSID, "isDemo": self.account_type, "tournamentId": 0}
        data = f'42["authorization",{json.dumps(payload)}]'
        self.send_websocket_request(data)

        # ===== EVENT-DRIVEN FIX =====
        # polling:
        # while not check_accepted and not check_rejected:
        # if time.time() - start_time > timeout: return False
        # await asyncio.sleep(0.5)
        # accepted rejected .
        try:
            idx = await wait_for_first_event(
                self.state.auth_accepted_event,
                self.state.auth_rejected_event,
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            return False
        return idx == 0  # True accepted, False rejected

    async def connect(self, is_demo):
        self.account_type = 1 if is_demo else 0
        self.state.ssl_Mutual_exclusion = False
        self.state.ssl_Mutual_exclusion_write = False
        check_websocket, websocket_reason = await self.start_websocket()
        if not check_websocket: return check_websocket, websocket_reason
        check_ssid = await self.send_ssid()
        if not check_ssid:
            await self.authenticate()
            if self.state.SSID: await self.send_ssid()
        return check_websocket, websocket_reason

    async def close(self):
        if self.websocket_client and self.websocket_client.wss:
            self.websocket_client.wss.close()
            await asyncio.sleep(1)
        if self.websocket_thread and self.websocket_thread.is_alive():
            self.websocket_thread.join(timeout=5)
        return True

# ==============================================================================
# SECTION 7: QUOTEX STABLE API WITH FULL HISTORY MIXIN
# ==============================================================================
_request_counter = itertools.count(int(time.time() * 1000))

def group_by_period(data, period):
    grouped = defaultdict(list)
    for tick in data:
        timestamp = int(tick[0])
        timeframe = int(timestamp // period)
        grouped[timeframe].append(tick)
    return dict(grouped)

def calculate_candles(history, period):
    if not isinstance(history, list) or not history: return []
    grouped = group_by_period(history, period)
    candles = []
    for minute, ticks in grouped.items():
        open_price = ticks[0][1]
        close_price = ticks[-1][1]
        high_price = max(tick[1] for tick in ticks)
        low_price = min(tick[1] for tick in ticks)
        candle = {'time': minute * period, 'open': open_price, 'close': close_price, 'high': high_price, 'low': low_price, 'ticks': len(ticks)}
        candles.append(candle)
    return candles[:-1] if len(candles) > 1 else candles


# ===== FIX A: =====
# : calculate_candles "tick" [time, price]
# tick[1] open/high/low/close. Quotex
# "history/load" [time, open, close, high, low] (5 ).
# calculate_candles tick :
# open = high = low = close = open_price => !
# fill_gap_once => get_candles => prepare_candles.
# .
def _parse_raw_candles(raw_candles):
    """ WebSocket dicts OHLC .

 "history/load":
 1) [time, open, close, high, low] — Quotex (5 )
 2) [time, open, close, high, low, vol] — volume (6 )
 3) {"time", "open", "high", "low", "close", ...} — dict
 4) [time, price(, volume)] — tick ( )
 """
    if not raw_candles: return []
    parsed = []
    for c in raw_candles:
        try:
            if isinstance(c, dict) and 'time' in c:
                t = int(c.get('time', c.get('timestamp', 0)))
                o = float(c.get('open', 0) or 0)
                h = float(c.get('high', c.get('max', 0)) or 0)
                l = float(c.get('low', c.get('min', 0)) or 0)
                cl = float(c.get('close', c.get('c', 0)) or 0)
                v = int(c.get('volume', c.get('vol', 0)) or 0)
                if t > 0 and o > 0 and h > 0 and l > 0 and cl > 0:
                    parsed.append({'time': t, 'open': o, 'high': h, 'low': l, 'close': cl, 'volume': v})
            elif isinstance(c, (list, tuple)) and len(c) >= 5:
                # Quotex WS: [time, open, close, high, low(, volume)]
                t = int(c[0])
                o = float(c[1])
                cl = float(c[2])
                h = float(c[3])
                l = float(c[4])
                v = int(c[5]) if len(c) >= 6 else 0
                if t > 0 and o > 0 and h > 0 and l > 0 and cl > 0:
                    parsed.append({'time': t, 'open': o, 'high': h, 'low': l, 'close': cl, 'volume': v})
            elif isinstance(c, (list, tuple)) and len(c) >= 2:
                # tick format: [time, price(, volume)]
                t = int(c[0])
                p = float(c[1])
                v = int(c[2]) if len(c) >= 3 else 0
                if t > 0 and p > 0:
                    parsed.append({'time': t, 'open': p, 'high': p, 'low': p, 'close': p, 'volume': v})
        except (TypeError, ValueError):
            continue
    return parsed

def process_candles_v2(history, asset, data):
    if not history or not isinstance(history, dict): return data if data else []
    candles_data = history.get(asset, {})
    candles = candles_data.get("candles", [])[1:] if candles_data else []
    combined = candles + (data if data else [])
    if combined:
        candle_dict = {c.get('time'): c for c in combined if isinstance(c, dict) and 'time' in c}
        return list(candle_dict.values()) if candle_dict else []
    return combined

def merge_candles(candles_data):
    if not candles_data: return []
    candle_dict = {c['time']: c for c in candles_data if isinstance(c, dict) and 'time' in c}
    return sorted(candle_dict.values(), key=lambda x: x['time']) if candle_dict else []

class Quotex:
    def __init__(self, email=None, password=None, host="quotex.com", lang="en", proxies=None, user_data_dir="browser", asset_default="EURUSD", period_default=60):
        self.email = email
        self.password = password
        self.host = host
        self.lang = lang
        self.proxies = proxies
        self.user_data_dir = user_data_dir
        self.asset_default = asset_default
        self.period_default = period_default
        self.account_is_demo = 1
        self.codes_asset = {}
        self.api = None
        self.subscribe_candle = []
        self.subscribe_candle_all_size = []
        self.subscribe_mood = []
        session = load_session(self.email, USER_AGENT)
        self.session_data = session

    async def check_connect(self):
        if self.api is None: return False
        # ===== EVENT-DRIVEN FIX =====
        # :
        # await asyncio.sleep(1)
        # return self.api.state.check_accepted_connection == 1
        # . 2s 50ms
        # — latency 1000ms <100ms.
        try:
            await wait_until(
                lambda: self.api.state.check_accepted_connection == 1,
                timeout=2.0,
                poll_interval=0.05,
            )
            return True
        except asyncio.TimeoutError:
            return self.api.state.check_accepted_connection == 1

    async def connect(self):
        self.api = QuotexAPI(self.host, self.email, self.password, self.lang, proxies=self.proxies, user_data_dir=self.user_data_dir)
        self.api.session_data = self.session_data
        self.api.state.SSID = self.session_data.get("token")
        self.api._async_loop = asyncio.get_running_loop()
        if not self.session_data.get("token"):
            check, reason = await self.api.authenticate()
            if not check: return check, reason
        check, reason = await self.api.connect(self.account_is_demo == 1)
        if not check:
            self.session_data = {}
            return False, "Websocket connection rejected."
        return check, reason

    async def change_account(self, balance_mode: str):
        self.account_is_demo = 0 if balance_mode.upper() == "REAL" else 1
        self.api.account_type = self.account_is_demo
        payload = {"demo": self.api.account_type, "tournamentId": 0}
        self.api.send_websocket_request(f'42["account/change",{json.dumps(payload)}]')

    async def get_instruments(self, timeout: float = 12.0):
        """طريقة pyquotex الرسمية: طلب قائمة instruments الكاملة عبر WebSocket
        والانتظار حتى وصولها (event: instruments_ready)."""
        if self.api is None:
            return []
        if self.api.instruments and len(self.api.instruments) > 0:
            return self.api.instruments
        try:
            await self.api.event_registry.clear_event("instruments_ready")
        except Exception:
            pass
        try:
            self.api.send_websocket_request('42["instruments/get"]')
        except Exception as e:
            logmsg(f"[instruments] request failed: {e}")
            return []
        data = await self.api.event_registry.wait_event("instruments_ready", timeout=timeout)
        if isinstance(data, dict) and data.get("_placeholder"):
            data = await self.api.event_registry.wait_event("instruments_ready", timeout=5.0)
        if isinstance(data, list) and data:
            self.api.instruments = data
        return list(self.api.instruments or [])

    async def get_all_assets(self):
        """طريقة pyquotex الرسمية: خريطة اسم الأصل -> كوده الداخلي."""
        if self.api is None:
            return {}
        try:
            await self.get_instruments()
        except Exception:
            pass
        for i in (self.api.instruments or []):
            try:
                if isinstance(i, (list, tuple)) and i[0] != "":
                    self.codes_asset[i[1]] = i[0]
            except Exception:
                continue
        return self.codes_asset

    def get_payment(self):
        """طريقة pyquotex الرسمية get_payment: نسبة الدفع + الحالة لكل الأصول."""
        if self.api is None or not self.api.instruments:
            return {}
        assets_data = {}
        for i in self.api.instruments:
            try:
                assets_data[str(i[2]).replace("\n", "")] = {
                    "turbo_payment": i[18],
                    "payment": i[5],
                    "profit": {"24H": i[-10], "1M": i[-9], "5M": i[-8]},
                    "open": i[14],
                }
            except Exception:
                continue
        return assets_data

    def get_payout_by_asset(self, asset_name: str, timeframe: str = "1"):
        """طريقة pyquotex الرسمية get_payout_by_asset: نسبة الدفع لأصل محدد.
        timeframe: "1" (1M) أو "5" (5M) أو "24" (24H) أو "all"."""
        if self.api is None or not self.api.instruments:
            return None
        data = None
        for i in self.api.instruments:
            try:
                if isinstance(i, (list, tuple)) and len(i) > 1 and asset_name == i[1]:
                    data = {
                        "turbo_payment": i[18] if len(i) > 18 else None,
                        "payment": i[5] if len(i) > 5 else None,
                        "profit": {"24H": i[-10], "1M": i[-9], "5M": i[-8]},
                        "open": i[14] if len(i) > 14 else None,
                    }
                    break
            except Exception:
                continue
        if data is None:
            return None
        if timeframe == "all":
            return data.get("profit")
        profit = data.get("profit")
        if profit:
            if str(timeframe) == "24":
                return profit.get("24H")
            return profit.get(f"{timeframe}M")
        return None

    async def start_candles_stream(self, asset="EURUSD", period=60):
        if self.api:
            self.api.current_asset = asset
            self.api.current_period = period
            self.api.subscribe_realtime_candle(asset, period)
            self.api.chart_notification(asset)
            self.api.follow_candle(asset)

    async def get_realtime_candles(self, asset: str):
        if self.api: return self.api.realtime_candles.get(asset, [])
        return []

    async def get_candles(self, asset, end_from_time, offset, period, progressive=False, timeout=30, use_cache=False):
        if self.api is None: return None
        if end_from_time is None: end_from_time = time.time()
        index = calendar.timegm(time.gmtime())
        self.api.candles.candles_data = None
        await self.api.event_registry.clear_event(f'candles_ready_{asset}')
        await self.start_candles_stream(asset, period)
        self.api.get_candles_ws(asset, index, int(end_from_time), offset, period)
        try:
            history_data = await self.api.event_registry.wait_event(f'candles_ready_{asset}', timeout=timeout)
        except Exception:
            return None
        if history_data is None: return None
        candles = self.prepare_candles(asset, period, history_data)
        if progressive: return self.api.historical_candles.get("data", {})
        return candles

    async def _fetch_historical_batch(self, asset, fetch_time, offset, period, index, timeout):
        if self.api is None: return None
        payload = {"asset": asset, "index": index, "time": fetch_time, "offset": offset, "period": period}
        ws_msg = f'42["history/load",{json.dumps(payload)}]'
        event_name = f'candles_ready_{asset}_{index}'
        await self.api.event_registry.clear_event(event_name)
        self.api.send_websocket_request(ws_msg)
        try:
            return await self.api.event_registry.wait_event(event_name, timeout=timeout)
        except Exception:
            return None

    def _parse_historical_candles(self, raw_data):
        if raw_data is None: return []
        raw_candles = raw_data.get("data", []) or raw_data.get("candles", [])
        if not raw_candles: return []
        parsed = []
        for c in raw_candles:
            if isinstance(c, list) and len(c) >= 5:
                parsed.append({"time": int(c[0]), "open": float(c[1]), "close": float(c[2]), "high": float(c[3]), "low": float(c[4])})
            elif isinstance(c, dict) and "time" in c:
                parsed.append(c)
        return parsed

    async def get_historical_candles(self, asset, amount_of_seconds, period, timeout=30, max_workers=5, progress_callback=None):
        # ===== FIX 7: (5 workers + chunk=200) =====
        # .
        # (keepalive + watchdog ).
        max_workers = max_workers or 1
        chunk_seconds = period * FETCH_CHUNK_SIZE  # period * 200 = 12000s batch
        all_candles = {}
        current_time = int(time.time())
        target_start_time = current_time - amount_of_seconds
        block_size = amount_of_seconds // max_workers
        semaphore = asyncio.Semaphore(max_workers)

        async def worker(start_t, end_t, worker_id):
            worker_candles = {}
            async with semaphore:
                oldest_t = start_t
                consecutive_failures_in_worker = 0
                while oldest_t > end_t:
                    # ===== FIX 3: batch =====
                    if not self.api or not getattr(self.api.state, 'check_accepted_connection', False):
                        # —
                        break
                    index = next(_request_counter)
                    batch_data = await self._fetch_historical_batch(asset, oldest_t, chunk_seconds, period, index, timeout)
                    if not batch_data:
                        oldest_t -= chunk_seconds
                        consecutive_failures_in_worker += 1
                        if consecutive_failures_in_worker >= 3:
                            # 3 consecutive failures = connection dead — exit
                            break
                        # backoff
                        await asyncio.sleep(FETCH_BATCH_DELAY * 2)
                        continue
                    consecutive_failures_in_worker = 0
                    new_batch = self._parse_historical_candles(batch_data)
                    if not new_batch: oldest_t -= chunk_seconds; continue
                    batch_times = []
                    for c in new_batch:
                        ts = c['time']
                        if ts >= end_t and ts <= start_t:
                            worker_candles[ts] = c
                            batch_times.append(ts)
                    if not batch_times: oldest_t -= chunk_seconds; continue
                    batch_times.sort()
                    new_oldest = batch_times[0]
                    if progress_callback: progress_callback(start_t - new_oldest, start_t - end_t, len(worker_candles), f"Worker-{worker_id}")
                    oldest_t = new_oldest if new_oldest < oldest_t else oldest_t - chunk_seconds
                    # batches (0.1s — )
                    await asyncio.sleep(FETCH_BATCH_DELAY)
            return list(worker_candles.values())

        await self.start_candles_stream(asset, period)
        tasks = []
        for i in range(max_workers):
            s = current_time - (i * block_size)
            e = max(target_start_time, s - block_size)
            tasks.append(worker(s, e, i))
        results = await asyncio.gather(*tasks)
        for batch in results:
            for c in batch: all_candles[c['time']] = c
        return sorted(all_candles.values(), key=lambda x: x['time'])

    def prepare_candles(self, asset, period, history=None):
        if self.api is None: return []
        history_data = history if history is not None else self.api.candles.candles_data
        if history_data is None: return []
        if isinstance(history_data, dict):
            candles_list = history_data.get("candles") or history_data.get("data") or history_data.get("history") or []
        else:
            candles_list = history_data
        if not candles_list: return []
        # ===== FIX A: _parse_raw_candles calculate_candles =====
        # calculate_candles tick [time, price]
        # tick[1] open/high/low/close — (open == high ==
        # low == close) history/load
        # [time, open, close, high, low]. _parse_raw_candles
        # .
        candles_data = _parse_raw_candles(candles_list)
        # grill period ( )
        for c in candles_data:
            c['time'] = (int(c['time']) // period) * period
        candles_v2_data = process_candles_v2(self.api.candle_v2_data, asset, candles_data)
        return merge_candles(candles_v2_data)

    async def close(self):
        if self.api: return await self.api.close()
        return True

# ==============================================================================
# SECTION 8: LOGGING & UI HELPERS (MT4 WRITER REMOVED — v11)
# ==============================================================================
os.environ['SSL_CERT_FILE'] = cert_path
os.environ['WEBSOCKET_CLIENT_CA_BUNDLE'] = cert_path

DEBUG_LOGS = False
def dbg(msg: str):
    if DEBUG_LOGS: print(f"  \033[2m   [debug] {msg}\033[0m")

LOG_FILE = Path("qxchart.log")
def _log_to_file(line: str):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f: f.write(line + "\n")
    except Exception: pass

def logmsg(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    _log_to_file(line)
    # If live table is active, queue the message instead of printing directly
    # to avoid corrupting the redrawn table
    if '_STATUS_MESSAGES_QUEUE' in globals() and globals()['_STATUS_MESSAGES_QUEUE'] is not None:
        try:
            globals()['_STATUS_MESSAGES_QUEUE'].append(line)
            # keep last 5 messages
            if len(globals()['_STATUS_MESSAGES_QUEUE']) > 5:
                globals()['_STATUS_MESSAGES_QUEUE'] = globals()['_STATUS_MESSAGES_QUEUE'][-5:]
        except Exception:
            pass
    else:
        print(f"  \033[2m[{ts}]\033[0m {msg}")

def log_exception(context: str, exc: BaseException):
    ts = datetime.now().strftime("%H:%M:%S")
    try:
        tb_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        # Safe encoding - ignore problematic characters
        error_msg = str(exc).encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
        print(f"  \033[91m[{ts}] FATAL in {context}: {error_msg}\033[0m")
        _log_to_file(f"[{ts}] FATAL in {context}: {error_msg}\n{tb_text}")
    except Exception as e:
        # Fallback if even the error handler fails
        try:
            print(f"  \033[91m[{ts}] FATAL in {context}: Encoding error occurred\033[0m")
            _log_to_file(f"[{ts}] FATAL in {context}: Encoding error - {e}")
        except:
            pass

def _thread_excepthook(args):
    log_exception(f"thread '{args.thread.name}'", args.exc_value)
threading.excepthook = _thread_excepthook

def _main_excepthook(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt): sys.__excepthook__(exc_type, exc_value, exc_tb); return
    # Transient encoding errors should NOT kill the whole process.
    if issubclass(exc_type, UnicodeError):
        try:
            ts = datetime.now().strftime("%H:%M:%S")
            safe = str(exc_value).encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
            print(f"  \033[93m[{ts}] SWALLOWED UnicodeError at top level: {safe}\033[0m")
            _log_to_file(f"[{ts}] SWALLOWED UnicodeError at top level: {safe}")
        except Exception:
            pass
        return
    log_exception("main thread (top level)", exc_value)
sys.excepthook = _main_excepthook

class _NullHandler(logging.Handler):
    def emit(self, record): pass

@contextlib.contextmanager
def suppress_terminal_output():
    _loggers = [logging.getLogger(), logging.getLogger("pyquotex"), logging.getLogger("Quotex"), logging.getLogger("websockets"), logging.getLogger("websocket"), logging.getLogger("asyncio")]
    _old_levels = [(l, l.level, l.disabled) for l in _loggers]
    _old_handlers = [(l, list(l.handlers)) for l in _loggers]
    for l in _loggers:
        l.handlers = [_NullHandler()]; l.setLevel(logging.CRITICAL + 1); l.disabled = True
    with open(os.devnull, 'w') as devnull:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = devnull, devnull
        try: yield
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr
            for l, lvl, dis in _old_levels: l.setLevel(lvl); l.disabled = dis
            for l, hs in _old_handlers: l.handlers = hs

# (تم نزع MT4) — كانت هنا find_mt4_window/refresh_mt4 عبر WinAPI لإعادة رسم
# شارت MT4؛ أُزيلت بالكامل في نسخة v11 بدون MT4.

class Colors:
    GREEN = '\033[92m'; RED = '\033[91m'; BLUE = '\033[94m'; YELLOW = '\033[93m'
    CYAN = '\033[96m'; BOLD = '\033[1m'; DIM = '\033[2m'; RESET = '\033[0m'; CLEAR_LINE = '\033[K'
    # ===== ألوان إضافية مطلوبة للواجهة الجديدة (v3 UI) =====
    WHITE = '\033[97m'      # أبيض ساطع
    MAGENTA = '\033[95m'    # بنفسجي ساطع
    GRAY = '\033[90m'       # رمادي

def _current_period_label() -> str:
    # Subsecond labels
    if IS_SUBSECOND and SUBSECOND_LABEL:
        return SUBSECOND_LABEL
    if PERIOD >= 1440 and PERIOD % 1440 == 0:
        d = PERIOD // 1440
        return f"D{d}" if d > 1 else "D1"
    if PERIOD >= 60 and PERIOD % 60 == 0:
        h = PERIOD // 60
        return f"H{h}" if h > 1 else "H1"
    return f"M{PERIOD}"

def pretty_asset_name(symbol: str, period_label: str = None) -> str:
    if period_label is None:
        period_label = _current_period_label()
    base = symbol
    suffix = ""
    if base.upper().endswith("_OTC"):
        base = base[:-4]
        suffix = " · OTC"
    if len(base) == 6 and base.isalpha():
        pretty = f"{base[:3].upper()}/{base[3:].upper()}{suffix}"
    else:
        pretty = f"{base.upper()}{suffix}"
    return f"({pretty}),{period_label}"

# (تم نزع MT4) — mt4_config.json و mt4_digits.json و find_mt4_history_path
# و MT4_HISTORY_PATH حُذفت بالكامل؛ لا يعتمد البرنامج على مجلد MT4 إطلاقاً.
# Credentials, session state and login state now live in app_data.db
# (see SECTION 2B). The three constants below are kept only as legacy
# references for any external code that still imports them; they are no
# longer used for storage.
SESSION_FILE = Path("session.json")
SESSION_STATE_FILE = Path("session_state.json")
EMAIL_FILE = Path("saved_email.txt")

class SessionManager:
    def __init__(self): self.state = self._load_state()
    def _load_state(self) -> dict:
        try:
            with _db_lock:
                conn = _db_conn()
                try:
                    row = conn.execute("SELECT * FROM app_state WHERE id=1").fetchone()
                    if row:
                        return {
                            "last_login": row["last_login"] or 0,
                            "last_success": bool(row["last_success"]),
                            "failed_auth_count": row["failed_auth_count"] or 0,
                            "email": row["email"] or "",
                            "password": row["password"] or "",
                            "host": row["host"] or "qxbroker.com",
                        }
                finally:
                    conn.close()
        except Exception:
            pass
        return {"last_login": 0, "last_success": False, "failed_auth_count": 0, "email": "", "password": "", "host": "qxbroker.com"}
    def _save_state(self):
        try:
            with _db_lock:
                conn = _db_conn()
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO app_state
                        (id, email, password, host, last_login, last_success, failed_auth_count)
                        VALUES (1, ?, ?, ?, ?, ?, ?)
                    """, (
                        1,
                        self.state.get("email", ""),
                        self.state.get("password", ""),
                        self.state.get("host", "qxbroker.com"),
                        self.state.get("last_login", 0),
                        1 if self.state.get("last_success", False) else 0,
                        self.state.get("failed_auth_count", 0),
                    ))
                    conn.commit()
                finally:
                    conn.close()
        except Exception:
            pass
    def should_force_fresh(self) -> bool:
        if not self.state.get("last_success", False): return True
        if self.state.get("failed_auth_count", 0) >= 2: return True
        if time.time() - self.state.get("last_login", 0) > 7 * 24 * 3600: return True
        return False
    def record_success(self, email: str, password: str = "", host: str = None):
        # Save credentials (email + password) and login state in app_data.db.
        # password defaults to whatever was already stored, so callers that
        # don't pass it explicitly won't wipe the saved value.
        update_dict = {
            "last_login": time.time(),
            "last_success": True,
            "failed_auth_count": 0,
            "email": email,
            "password": password or self.state.get("password", ""),
        }
        if host: update_dict["host"] = host
        self.state.update(update_dict)
        self._save_state()
    def record_failure(self, error: str):
        self.state["last_login"] = time.time()
        self.state["last_success"] = False
        self.state["failed_auth_count"] = self.state.get("failed_auth_count", 0) + 1
        self._save_state()
    def get_saved_email(self) -> str:
        email = self.state.get("email", "")
        return email if email else ""
    def get_saved_password(self) -> str:
        return self.state.get("password", "")
    def get_saved_host(self) -> str:
        return self.state.get("host", DEFAULT_DOMAIN_GLOBAL)
    @staticmethod
    def delete_session_file():
        # Clear the sessions table so the next login performs a fresh
        # authentication (no stale cookies/token). The credentials row in
        # app_state is intentionally preserved so the user doesn't have to
        # retype email/password.
        try:
            with _db_lock:
                conn = _db_conn()
                try:
                    conn.execute("DELETE FROM sessions")
                    conn.commit()
                    return True
                finally:
                    conn.close()
        except Exception:
            return False
    @staticmethod
    def delete_browser_dir():
        browser_dir = Path("browser")
        if browser_dir.exists():
            try: shutil.rmtree(browser_dir, ignore_errors=True); return True
            except Exception: pass
        return False

session_manager = SessionManager()

# ==============================================================================
# UPDATED ASSET LIST & CANDLE COUNT SETTINGS
# ==============================================================================
ASSET_LIST = [
    # العملة الافتراضية الوحيدة: عند التشغيل والاتصال يبدأ بجلب شموعها (0.80 يوم)
    # وبعد اكتمال جلب شموعها يطبع قائمة بقية العملات (نسبة الدفع + مفتوح/مغلق)
    "BRLUSD_otc",
]
DEFAULT_ASSET = ASSET_LIST[0]

TIMEFRAMES = [
    (1,    60,      "M1  - 1 minute",      False),
    (2,    120,     "M2  - 2 minutes",     False),
    (3,    180,     "M3  - 3 minutes",     False),
    (5,    300,     "M5  - 5 minutes",     False),
    (10,   600,     "M10 - 10 minutes",    False),
    (15,   900,     "M15 - 15 minutes",    False),
    (30,   1800,    "M30 - 30 minutes",    False),
    (60,   3600,    "H1  - 1 hour",        False),
    (240,  14400,   "H4  - 4 hours",       False),
    (1440, 86400,   "D1  - 1 day",         False),
    # Subsecond timeframes (aggregated locally from realtime ticks, no history API)
    (1,    5,       "S5  - 5 seconds",     True),
    (1,    10,      "S10 - 10 seconds",    True),
    (1,    15,      "S15 - 15 seconds",    True),
    (1,    30,      "S30 - 30 seconds",    True),
]

PERIOD = 1
PERIOD_SECONDS = 60
# Subsecond mode globals — set when user chooses S5/S10/S15/S30
IS_SUBSECOND = False
SUBSECOND_SECONDS = 0   # 5, 10, 15, or 30
SUBSECOND_LABEL = ""    # "5S", "10S", "15S", "30S"
SUBSECOND_MAX_CANDLES = 4000  # keep last 4000 subsecond candles in memory (rolling) — v15: raised from 200
SUBSECOND_HISTORY_CANDLES = 500  # fetch up to 500 historical subsecond candles at startup (was 1000, reduced for speed)
# Semaphore to limit concurrent subsecond history fetches (3 at a time, not 41)
# Created lazily on first use to avoid event loop binding issues at import time
_SUBSECOND_FETCH_SEMAPHORE = None
# ===== FIX 4: 1000 ( ) =====
# 500 1000 (4s).
# : workers=5 + chunk=200 ( ) .
INITIAL_CANDLES = 1000
MIN_CANDLES_THRESHOLD = 100
HISTORY_DAYS = 0.75
FETCH_DURATION_SECONDS = int(86400 * HISTORY_DAYS)
STREAM_POLL_INTERVAL = 0.15
# (تم نزع MT4) — WRITE_INTERVAL كان لـ WriterThread/HST وأُزيل بالكامل
STALE_DATA_TIMEOUT = 180
STALE_RATIO_TRIGGER = 0.8
# ===== FIX 7: KEEPALIVE_PING_INTERVAL = 5s =====
# : " ".
# 10s (5 workers × 41 )
# keepalive . 5s .
KEEPALIVE_PING_INTERVAL = 5
RECONNECT_CHECK_INTERVAL = 1
RECONNECT_BASE_DELAY = 2
RECONNECT_MAX_DELAY = 30
STATUS_REFRESH_INTERVAL = 0.5
# تحديث السعر كل تيكس: أحداث التيكس توقظ حلقة رسم الجدول الحي فوراً
TICK_UPDATE_EVENT = threading.Event()
# ===== FIX 7: watchdog =====
# .
# STALE_MESSAGE_TIMEOUT=45s + STALE_WATCHDOG_INTERVAL=5s
# => latency = 45-50s ( )
# FIX 7: STALE_MESSAGE_TIMEOUT=20s + STALE_WATCHDOG_INTERVAL=3s
# => latency = 20-23s ( ~2x)
STALE_MESSAGE_TIMEOUT = 20
STALE_WATCHDOG_INTERVAL = 3  # 3s (was 5s)
ENGINEIO_PING_INTERVAL = 20  # "2" (engine.io ping) 20s — Socket.IO EIO=3
# ===== FIX 4: 4s 1000 =====
# ( 4s 1000 ):
# max_workers=5, chunk=period*200 (12000s), delay=0.1s
# : :
# - FETCH_ASSET_DELAY (0.1s)
# - workers ( FIX 3)
# - exponential backoff retry ( FIX 3)
# - main loop ( FIX 3)
# : 4s + ( FIX 1+2 )
FETCH_MAX_WORKERS = 5          # 5 workers (reverted — 10 caused connection drops)
FETCH_CHUNK_SIZE = 200         # 200 candles per batch (reverted)
FETCH_BATCH_DELAY = 0.1        # 0.1s between batches (reverted)
FETCH_ASSET_DELAY = 0.1        # 0.1s between assets (reverted)
# ===== FIX 7: cooldown — =====
FETCH_COOLDOWN_EVERY = 0       # 0 =
FETCH_COOLDOWN_DURATION = 0.0  # 0 =
FETCH_TIMEOUT = 30             # 30s per batch (reverted)
RETRY_BACKOFF_BASE = 2         # exponential backoff base
RETRY_BACKOFF_MAX = 15         # max delay between retries (reverted)
MAX_FETCH_RETRIES = 5          # 5 attempts (reverted)

MAX_RECONNECTS_DURING_FETCH = 8
# ===== FIX L1: gap fill 120 () 20 =====
# : " "
# 20 (20 ) 20 — .
# 120 () /.
GAP_FILL_CANDLES = 10  # fetch 10 recent candles to replace incomplete ones
# ===== FIX L1: 5 ( ) =====
# : _gap_filled = True => .
# : GAP_FILL_REFRESH_INTERVAL .
GAP_FILL_REFRESH_INTERVAL = 300  # 5

# ==============================================================================
# ===== BRIDGE FETCH (سد فجوة 4-5 شموع بين التاريخي والبث المباشر) =====
# ==============================================================================
# المشكلة: بعد انتهاء الجلب التاريخي (الذي يستغرق 5-15 ثانية)، يكون البث قد بدأ
#   لكن الخادم يؤجل إرسال ticks بسبب ضغط طلبات history/load. النتيجة: فجوة 4-5 شموع.
# الحل: بعد اكتمال الجلب التاريخي، اطلب آخر شموع تنتهي عند time.time() الحالي
#   (وليس T0). هذا يملأ الفجوة بين آخر شمعة تاريخية وأول شمعة بث مباشر.
# الأمان:
#   - BRIDGE_FETCH_ENABLED=False يُعطّل الميزة فوراً دون أي تأثير جانبي
#   - كل الأعمدة محاطة بـ try/except (الخطأ لا يُكسر الجلب التاريخي)
#   - Semaphore يمنع إرهاق الخادم بطلبات جسر متزامنة
#   - يستخدم get_candles الموجودة (دالة مُجرّبة وآمنة)
BRIDGE_FETCH_ENABLED = True       # مفتاح التشغيل/الإيقاف الرئيسي
BRIDGE_FETCH_CANDLES = 15         # عدد الشموع لجلب الجسر (يكفي لفجوة كبيرة + هامش أمان)
BRIDGE_FETCH_TIMEOUT = 20         # مهلة الجلب الجسري (ثانية) — زادت من 15 إلى 20
BRIDGE_FETCH_SEMAPHORE_LIMIT = 2  # أقصى عدد طلبات جسر متزامنة (لمنع إرهاق الخادم)
_BRIDGE_FETCH_SEMAPHORE = None    # يُنشأ تلقائياً عند أول استخدام (lazy init)

# ===== FIX L2: — =====
# : _is_dst time.altzone/time.timezone
# Windows ( DST
# DST — Africa/Algiers).
# : / HST → MT4.
# : datetime.now().astimezone().utcoffset() ( )
# .
try:
    from datetime import datetime as _dt_naive
    _SYSTEM_TZ_OFFSET = int(_dt_naive.now().astimezone().utcoffset().total_seconds())
except Exception:
    _SYSTEM_TZ_OFFSET = -time.timezone  # fallback ( )
# ===== " " : =====
# :
# LOCAL_TZ_OFFSET_SECONDS = 3600 # UTC+1 ( )
# LOCAL_TZ_OFFSET_SECONDS = 0 # UTC ( )
# LOCAL_TZ_OFFSET_SECONDS = 7200 # UTC+2 ( )
# LOCAL_TZ_OFFSET_SECONDS = 10800 # UTC+3 ( )
# None .
LOCAL_TZ_OFFSET_SECONDS = None  # None = _SYSTEM_TZ_OFFSET

def _get_tz_offset() -> int:
    """ (UTC → ). ."""
    if LOCAL_TZ_OFFSET_SECONDS is not None:
        return LOCAL_TZ_OFFSET_SECONDS
    return _SYSTEM_TZ_OFFSET

def _align_time(raw_ts: int, period_seconds: int = None) -> int:
    """Aligns a raw timestamp to period boundary after applying TZ offset.

 period_seconds=None reads PERIOD_SECONDS global dynamically (multi-timeframe support).
 """
    if period_seconds is None:
        period_seconds = PERIOD_SECONDS
    tz = _get_tz_offset()
    return ((int(raw_ts) + tz) // period_seconds) * period_seconds

# Fill missing slots with flat candles. max_gap=1440 (full day) allows filling
# gaps up to 1 day. period_seconds=None reads PERIOD_SECONDS dynamically.
def _fill_missing_slots(candles: list, period_seconds: int = None,
                        max_gap: int = 1440) -> list:
    """Fills time gaps between candles with flat candles (previous close).

 - Added candles have volume=0 (marked synthetic).
 - max_gap: max number of periods to fill (1440 = 1 day for M1).
 - period_seconds=None reads PERIOD_SECONDS dynamically (multi-timeframe).
 """
    if period_seconds is None:
        period_seconds = PERIOD_SECONDS
    if len(candles) < 2:
        return list(candles)
    candles = sorted(candles, key=lambda x: x['time'])
    out = [dict(candles[0])]
    for i in range(1, len(candles)):
        prev = out[-1]
        curr = candles[i]
        gap = (curr['time'] - prev['time']) // period_seconds
        if 0 < gap <= max_gap:

            slot = prev['time'] + period_seconds
            while slot < curr['time']:
                out.append({
                    'time': slot,
                    'open': prev['close'],
                    'high': prev['close'],
                    'low': prev['close'],
                    'close': prev['close'],
                    'volume': 0,
                })
                slot += period_seconds
        out.append(dict(curr))
    return out

# Aggregate 1-second candles into target_period (5s, 10s, 15s, 30s) candles.
# Used for sub-60s timeframes: fetch period=1 from server, aggregate locally.
def _aggregate_sub_minute(candles: list, target_period: int) -> list:
    """Aggregate 1-second candles into target_period candles.

    For each group of (target_period) consecutive 1s candles:
    - time = first candle's time aligned to target_period boundary
    - open = first candle's open
    - high = max of all highs
    - low = min of all lows
    - close = last candle's close
    - volume = sum of all volumes
    """
    if not candles or target_period <= 1:
        return list(candles)
    candles = sorted(candles, key=lambda x: x['time'])
    buckets = OrderedDict()
    for c in candles:
        bucket_time = (c['time'] // target_period) * target_period
        if bucket_time not in buckets:
            buckets[bucket_time] = {
                'time': bucket_time,
                'open': c['open'],
                'high': c['high'],
                'low': c['low'],
                'close': c['close'],
                'volume': int(c.get('volume', 0) or 0),
            }
        else:
            b = buckets[bucket_time]
            b['high'] = max(b['high'], c['high'])
            b['low'] = min(b['low'], c['low'])
            b['close'] = c['close']
            b['volume'] += int(c.get('volume', 0) or 0)
    return list(buckets.values())

# ==============================================================================
# ===== GEO DOMAIN (التعرف التلقائي على البلد عبر IP) — مأخوذ من الكود الأول =====
# ==============================================================================
# القاعدة (طلب المستخدم):
#   بنغلاديش (BD)  -> market-qx.info   (نطاق بنغلاديش)
#   أي دولة أخرى   -> quotex.com       (النطاق الرسمي العالمي)
# يتم التعرف على "البلد فقط" (وليس الموقع الدقيق) عبر IP عند كل تشغيل،
# بدون أي قائمة اختيار يدوية.
DEFAULT_DOMAIN_BANGLADESH = "market-qx.info"
DEFAULT_DOMAIN_GLOBAL = "quotex.com"
QX_HOST = DEFAULT_DOMAIN_GLOBAL
_GEO_CACHE = {"country": None, "checked_at": 0.0}

def detect_country_code(timeout: float = 4.0):
    """يرجع رمز البلد فقط (مثل BD / SA / EG) عبر IP العام. لا يجلب أي موقع دقيق."""
    if _GEO_CACHE["country"] and time.time() - _GEO_CACHE["checked_at"] < 3600:
        return _GEO_CACHE["country"]
    endpoints = [
        ("http://ip-api.com/json/?fields=status,countryCode", lambda d: (d or {}).get("countryCode")),
        ("https://ipapi.co/json/", lambda d: (d or {}).get("country_code")),
        ("https://api.ip.sb/geoip", lambda d: (d or {}).get("country_code")),
    ]
    for url, extract in endpoints:
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
            code = extract(r.json())
            if code:
                _GEO_CACHE["country"] = str(code).upper()
                _GEO_CACHE["checked_at"] = time.time()
                return _GEO_CACHE["country"]
        except Exception:
            continue
    return None

def resolve_qx_host() -> str:
    """يحدد النطاق تلقائياً حسب البلد ويضبط QX_HOST العام:
    بنغلاديش -> market-qx.info ، أي دولة أخرى (أو فشل التعرف) -> quotex.com."""
    global QX_HOST
    country = detect_country_code()
    if country == "BD":
        QX_HOST = DEFAULT_DOMAIN_BANGLADESH
    else:
        QX_HOST = DEFAULT_DOMAIN_GLOBAL
    return QX_HOST

# ==============================================================================
# ===== FULL ASSET LIST + PAYOUT % + OPEN/CLOSED (طريقة pyquotex الرسمية) =====
# ===== مأخوذ كما هو من الكود الأول =====
# ==============================================================================
# الطريقة الرسمية من مستودع pyquotex (cleitonleonel/pyquotex):
#   1) الإرسال عبر نفس WebSocket:  42["instruments/get"]
#   2) الرد يصل بصيغة:             451-["instruments/list", [ ... ]]
#   3) كل عنصر في القائمة يحتوي:
#        i[1]  = اسم الأصل (EURUSD_otc)     i[2]  = الاسم المعروض
#        i[5]  = نسبة الدفع payout %        i[14] = الحالة (True=مفتوح / False=مغلق)
#        i[18] = نسبة دفع التيربو turbo     i[-10],i[-9],i[-8] = أرباح 24H/1M/5M
# (طلب المستخدم) — لا مراقبة دورية ولا حفظ JSON خارجي؛ القائمة تُطبع مرة واحدة
# بعد اكتمال جلب شموع العملة الافتراضية BRLUSD_otc.

def _instruments_snapshot():
    """يبني قائمة الأصول الكاملة (الاسم + نسبة الدفع + الحالة) من instruments."""
    if CLIENT is None or CLIENT.api is None:
        return []
    insts = CLIENT.api.instruments or []
    out = []
    for i in insts:
        try:
            if not isinstance(i, (list, tuple)) or len(i) < 6:
                continue
            entry = {
                "asset": str(i[1]) if len(i) > 1 else "",
                "name": str(i[2]).replace("\n", " ").strip() if len(i) > 2 else "",
                "payout": i[5] if len(i) > 5 else None,
            }
            try:
                entry["id"] = str(i[0])
            except Exception:
                entry["id"] = ""
            try:
                entry["turbo_payout"] = i[18] if len(i) > 18 else None
            except Exception:
                entry["turbo_payout"] = None
            try:
                entry["open"] = bool(i[14]) if len(i) > 14 else None
            except Exception:
                entry["open"] = None
            # أرباح الفريمات (طريقة get_payment الرسمية): -10=24H ، -9=1M ، -8=5M
            try:
                entry["profit_24h"] = i[-10]
            except Exception:
                entry["profit_24h"] = None
            try:
                entry["profit_1m"] = i[-9]
            except Exception:
                entry["profit_1m"] = None
            try:
                entry["profit_5m"] = i[-8]
            except Exception:
                entry["profit_5m"] = None
            out.append(entry)
        except Exception:
            continue
    return out

async def fetch_instruments_now(timeout: float = 12.0):
    """طريقة pyquotex الرسمية: إرسال 42["instruments/get"] عبر نفس WebSocket
    والانتظار حتى وصول 451-["instruments/list"] ثم تخزينها في CLIENT.api.instruments.
    طريقة الدخول وطريقة جلب الشموع لا تتأثر إطلاقاً."""
    if CLIENT is None or CLIENT.api is None:
        return []
    # ===== v16: مسار سريع (نمط pyquotex) — إن كانت القائمة محفوظة من وقت الاتصال
    # (get_all_assets) نُرجعها فوراً بدون إعادة طلب ولا انتظار 15 ثانية =====
    if getattr(CLIENT.api, "instruments", None):
        return list(CLIENT.api.instruments)
    try:
        await CLIENT.api.event_registry.clear_event("instruments_ready")
    except Exception:
        pass
    try:
        CLIENT.api.send_websocket_request('42["instruments/get"]')
    except Exception as e:
        logmsg(f"[payout] instruments request failed: {e}")
    data = await CLIENT.api.event_registry.wait_event("instruments_ready", timeout=timeout)
    if isinstance(data, dict) and data.get("_placeholder"):
        # الرد كان placeholder — ننتظر الرد الحقيقي (نفس سلوك المكتبة الرسمية)
        data = await CLIENT.api.event_registry.wait_event("instruments_ready", timeout=5.0)
    if isinstance(data, list) and data:
        CLIENT.api.instruments = data
    return list(CLIENT.api.instruments or [])

def _print_payout_table(entries):
    """طباعة الجدول الكامل: العملة / نسبة الدفع / الحالة (مفتوح أو مغلق)."""
    open_entries = [e for e in entries if e.get("open")]
    closed_entries = [e for e in entries if e.get("open") is False]
    print(f"\n{Colors.CYAN}{'═' * 72}{Colors.RESET}")
    print(f"{Colors.BOLD}  FULL ASSET LIST — PAYOUT % — OPEN/CLOSED{Colors.RESET}")
    print(f"  Total: {len(entries)}  |  {Colors.GREEN}OPEN: {len(open_entries)}{Colors.RESET}"
          f"  |  {Colors.RED}CLOSED: {len(closed_entries)}{Colors.RESET}"
          f"  |  Domain: {QX_HOST}  |  Default: {DEFAULT_ASSET}")
    print(f"{Colors.CYAN}{'─' * 72}{Colors.RESET}")
    print(f"  {'ASSET':<16}{'PAYOUT':>7}  {'TURBO':>6}  {'1M':>5}  {'5M':>5}  STATUS")
    print(f"{Colors.DIM}{'─' * 72}{Colors.RESET}")
    for e in entries:
        name = (e.get("asset") or "?")[:16]
        payout = e.get("payout")
        turbo = e.get("turbo_payout")
        p1 = e.get("profit_1m")
        p5 = e.get("profit_5m")
        payout_str = f"{payout}%" if isinstance(payout, (int, float)) else "-"
        turbo_str = f"{turbo}%" if isinstance(turbo, (int, float)) else "-"
        p1_str = f"{p1}%" if isinstance(p1, (int, float)) else "-"
        p5_str = f"{p5}%" if isinstance(p5, (int, float)) else "-"
        if e.get("open"):
            status = f"{Colors.GREEN}OPEN{Colors.RESET}"
        else:
            status = f"{Colors.RED}CLOSED{Colors.RESET}"
        print(f"  {name:<16}{payout_str:>7}  {turbo_str:>6}  {p1_str:>5}  {p5_str:>5}  {status}")
    print(f"{Colors.CYAN}{'═' * 72}{Colors.RESET}")

# ===== v16: حفظ شموع كل عملة في ملف JSON بجانب السكربت مباشرة بعد اكتمال جلبها =====
def _save_candles_json(asset, source_label="fetch"):
    """يحفظ شموع العملة في JSON باسم: <العملة>_<الفريم>_<عدد الشموع>.json
    مثال: BRLUSD_otc_5S_720.json — ويُطبع مسار الملف بعد الحفظ."""
    try:
        candles = sorted(list(asset.candles), key=lambda c: c.get('time', 0))
        if not candles:
            return None
        count = len(candles)
        out_dir = Path(__file__).resolve().parent
        tf_label = _current_period_label()
        safe_symbol = str(asset.api_symbol).strip().replace(" ", "_")
        path = out_dir / f"{safe_symbol}_{tf_label}_{count}.json"
        payload = {
            "asset": asset.api_symbol,
            "display": pretty_asset_name(asset.api_symbol),
            "timeframe": tf_label,
            "period_seconds": PERIOD_SECONDS,
            "is_subsecond": bool(IS_SUBSECOND),
            "count": count,
            "history_method": HISTORY_METHOD,
            "source": source_label,
            "host": QX_HOST,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "fetched_at_unix": int(time.time()),
            "candles": candles,
        }
        with open(path, "w", encoding="utf-8") as _f:
            json.dump(payload, _f, ensure_ascii=False, indent=2)
        logmsg(f"[json] saved {path} ({count} candles)")
        return path
    except Exception as e:
        logmsg(f"[json] save failed for {asset.symbol}: {e}")
        return None

async def send_assets_list_once(timeout: float = 15.0):
    """يجلب قائمة العملات الكاملة من السيرفر (طريقة pyquotex الرسمية:
    42["instruments/get"] -> 451-["instruments/list"]) ويطبعها مرة واحدة
    مع نسبة الدفع وحالة كل عملة (مفتوح/مغلق).
    تُستدعى بعد اكتمال جلب شموع العملة الافتراضية BRLUSD_otc."""
    try:
        if CLIENT is None or CLIENT.api is None or not CONNECTION_ALIVE:
            logmsg("[assets] client not ready — asset list skipped")
            return
        await fetch_instruments_now(timeout=timeout)
        entries = _instruments_snapshot()
        if entries:
            _print_payout_table(entries)
        else:
            logmsg("[assets] empty instruments list received")
    except Exception as e:
        log_exception("send_assets_list_once", e)


ASYNC_LOOP = None
ASYNC_ENGINE_GENERATION = 0
_ENGINE_READY = threading.Event()
CONNECTION_ALIVE = False
CLIENT = None
EMAIL = None
PASSWORD = None
RECONNECTING = False
ALL_STREAMING_ASSETS = []
LAST_HEALTH_CHECK = 0
HEALTH_CHECK_INTERVAL = 20
# ===== FIX B: =====
# : " ".
# —
# 1000 .
LAST_CONNECTION_DROP_TIME = 0            # 0 =
LONG_DISCONNECT_THRESHOLD = 3600        # 3600s = 1
# ===== FIX F: =====
# mt4_gap_filler print()
# 0.5s →
# . : .
GAP_FILL_SUMMARY = ""  # ( mt4_gap_filler)
_STATUS_MESSAGES_QUEUE = []  # queue for log messages during live table (avoid corrupting redraw)
GAP_FILL_ACTIVE = False  # True

def _asyncio_loop_exception_handler(loop, context):
    exc = context.get("exception")
    if exc: log_exception(f"asyncio loop (gen {ASYNC_ENGINE_GENERATION})", exc)

def start_async_engine():
    global ASYNC_LOOP, ASYNC_ENGINE_GENERATION, CONNECTION_ALIVE
    while True:
        try:
            ASYNC_ENGINE_GENERATION += 1
            gen = ASYNC_ENGINE_GENERATION
            new_loop = asyncio.new_event_loop()
            new_loop.set_exception_handler(_asyncio_loop_exception_handler)
            asyncio.set_event_loop(new_loop)
            ASYNC_LOOP = new_loop
            _ENGINE_READY.set()
            if gen > 1:
                logmsg(f"[AsyncEngine] event loop recreated (gen {gen}). Rescheduling tasks...")
                CONNECTION_ALIVE = False
                asyncio.run_coroutine_threadsafe(health_monitor(), ASYNC_LOOP)
                asyncio.run_coroutine_threadsafe(auto_reconnect(), ASYNC_LOOP)
                asyncio.run_coroutine_threadsafe(keepalive_loop(), ASYNC_LOOP)
                # ===== FIX: stale watchdog loop =====
                asyncio.run_coroutine_threadsafe(stale_message_watchdog(), ASYNC_LOOP)
                for asset in ALL_STREAMING_ASSETS:
                    asset.stream_task = None
                    asset.streaming = False
            new_loop.run_forever()
        except Exception as e:
            log_exception(f"AsyncEngine (gen {ASYNC_ENGINE_GENERATION})", e)
            CONNECTION_ALIVE = False
            time.sleep(1)

async_thread = threading.Thread(target=start_async_engine, daemon=True, name="AsyncEngine")
async_thread.start()
_ENGINE_READY.wait(timeout=10)

class Asset:
    @staticmethod
    def _derive_mt4_symbol(raw_symbol: str) -> str:
        s = raw_symbol.strip()
        is_otc = s.upper().endswith("_OTC")
        if is_otc:
            base = s[:-4].upper()
            if PERIOD_SECONDS < 60:
                # Sub-minute: compact name "AUDUSDOTC5S" (11 chars, fits 12-byte header)
                result = f"{base}OTC{SUBSECOND_LABEL}"
            else:
                result = f"{base}-OTC"
        else:
            if PERIOD_SECONDS < 60:
                result = f"{s.upper()}{SUBSECOND_LABEL}"
            else:
                result = s.upper()
        return result[:12] if result else raw_symbol.upper()[:12]

    def __init__(self, symbol):
        self.api_symbol = symbol
        self.symbol = self._derive_mt4_symbol(symbol)
        self.period = PERIOD
        self.price = 0.0
        self.previous_price = 0.0
        # (تم نزع MT4) — لم تعد Asset تحمل مسار ملف .hst؛ الاسم الداخلي يبقى
        # مفاتيح التخزين في CandleStore/sync_state دون أي تغيير.
        self.candles = []
        self.last_write = 0.0
        self.updates = 0
        self.streaming = False
        self.stream_task = None
        self.last_update_time = 0
        self.last_saved_close = 0.0
        self.last_saved_candle_time = 0
        self.last_saved_candle_count = 0
        self.dirty = False
        self._gap_filled = False

    def has_new_data(self):
        if not self.candles: return False
        c = self.candles[-1]
        return (c['close'] != self.last_saved_close or c['time'] != self.last_saved_candle_time or len(self.candles) != self.last_saved_candle_count)

    def mark_dirty(self): self.dirty = True

    def write(self, force=False):
        """(تم نزع MT4) — كانت تكتب ملف HST؛ الآن لا تكتب أي ملف، وتحافظ فقط على
        حالة الحفظ الداخلية (last_saved_*) حتى يبقى سلوك المستدعين كما هو."""
        if not force and not self.has_new_data():
            return False
        if self.candles:
            self.last_saved_close = self.candles[-1]['close']
            self.last_saved_candle_time = self.candles[-1]['time']
            self.last_saved_candle_count = len(self.candles)
        self.dirty = False
        return True


# (تم نزع MT4) — WriterThread كان يكتب ملفات HST كل 500ms ويعيد رسم MT4؛
# أُزيل بالكامل. البث المباشر وجلب الشموع لا يعتمدون عليه إطلاقاً.
async def connect_quotex(email, password, force_fresh=False, max_attempts=3):
    global CLIENT, CONNECTION_ALIVE, EMAIL, PASSWORD
    EMAIL, PASSWORD = email, password
    for attempt in range(1, max_attempts + 1):
        try:
            if CLIENT:
                try:
                    close_task = asyncio.create_task(CLIENT.close())
                    await asyncio.wait([close_task], timeout=3.0)
                except Exception: pass
                finally: CLIENT = None
            await asyncio.sleep(0.3)
            if force_fresh or attempt > 1:
                session_manager.delete_session_file()
                session_manager.delete_browser_dir()
            # FIX K: QX_HOST ( )
            CLIENT = Quotex(email=email, password=password, host=QX_HOST, lang="en")
            with suppress_terminal_output():
                check, reason = await CLIENT.connect()
            if check:
                try:
                    await CLIENT.change_account("PRACTICE")
                    await asyncio.sleep(0.5)
                except Exception: pass
                try: await CLIENT.get_all_assets()
                except Exception: pass
                CONNECTION_ALIVE = True
                # FIX K: persist email + password + host in app_data.db so the
                # next launch can auto-login without re-prompting the user.
                session_manager.record_success(email, password=password, host=QX_HOST)
                return True
            else:
                error_msg = str(reason) if reason else "Unknown error"
                session_manager.record_failure(error_msg)
        except Exception as e:
            error_msg = str(e)[:200]
            session_manager.record_failure(error_msg)
            if attempt < max_attempts: await asyncio.sleep(5 * attempt)
    CONNECTION_ALIVE = False
    return False

async def keepalive_loop():
    # ===== FIX 7: keepalive (5s) + =====
    # : " ".
    # : keepalive_loop
    # `await asyncio.sleep(KEEPALIVE_PING_INTERVAL)` .
    # : `asyncio.sleep` (0.5s) .
    # keepalive .
    tick_count = 0
    last_ping_at = 0.0
    while True:
        await asyncio.sleep(0.5)  # 500ms ()
        now = time.time()
        # : ping
        if now - last_ping_at < KEEPALIVE_PING_INTERVAL:
            continue
        last_ping_at = now
        if not CONNECTION_ALIVE or CLIENT is None or CLIENT.api is None: continue
        try:
            # 1) engine.io ping ( Socket.IO EIO=3) —
            CLIENT.api.send_websocket_request("2", no_force_send=False)
            # 2) application-level tick —
            CLIENT.api.send_websocket_request('42["tick"]', no_force_send=False)
            # 3)
            if tick_count % 3 == 0:
                CLIENT.api.send_websocket_request('42["instruments/get"]', no_force_send=False)
            tick_count += 1
        except Exception:
            pass


async def stale_message_watchdog():
    # ===== FIX: watchdog + =====
    # WS STALE_MESSAGE_TIMEOUT (45s)
    # .
    # latency ~25s <5s.
    # FIX B: auto_reconnect
    # (> ) —
    # .
    global CONNECTION_ALIVE, LAST_CONNECTION_DROP_TIME
    while True:
        await asyncio.sleep(STALE_WATCHDOG_INTERVAL)
        if not CONNECTION_ALIVE or CLIENT is None or CLIENT.api is None:
            continue
        try:
            silent_for = time.time() - CLIENT.api.last_message_at
            if silent_for > STALE_MESSAGE_TIMEOUT:
                logmsg(
                    f"[stale_watchdog] no messages for {silent_for:.1f}s "
                    f"(>{STALE_MESSAGE_TIMEOUT}s); recycling connection."
                )
                # ===== FIX B: =====
                # LONG_DISCONNECT_THRESHOLD (1h)
                # .
                if LAST_CONNECTION_DROP_TIME == 0:
                    LAST_CONNECTION_DROP_TIME = time.time()
                    logmsg(f"[stale_watchdog] drop time recorded at {LAST_CONNECTION_DROP_TIME:.0f}")
                CONNECTION_ALIVE = False
                # WS close frame on_close
                try:
                    if CLIENT.api.websocket_client and CLIENT.api.websocket_client.wss:
                        CLIENT.api.websocket_client.wss.close()
                except Exception:
                    pass
                # auto_reconnect
        except Exception as e:
            logmsg(f"[stale_watchdog] error: {e}")

async def health_monitor():
    global LAST_HEALTH_CHECK, CONNECTION_ALIVE
    consecutive_dead_checks = 0
    while True:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)
        now = time.time()
        LAST_HEALTH_CHECK = now
        if not CONNECTION_ALIVE or CLIENT is None:
            consecutive_dead_checks = 0
            continue
        is_dead = False
        dead_reason = ""
        stale_count = sum(1 for a in ALL_STREAMING_ASSETS if a.updates > 0 and (now - a.last_update_time) > STALE_DATA_TIMEOUT)
        if stale_count > len(ALL_STREAMING_ASSETS) * STALE_RATIO_TRIGGER:
            is_dead = True
            dead_reason = f"stale ratio {stale_count}/{len(ALL_STREAMING_ASSETS)} exceeded {STALE_RATIO_TRIGGER}"
        if is_dead:
            logmsg(f"[health_monitor] marking connection DEAD: {dead_reason}")
            CONNECTION_ALIVE = False
            consecutive_dead_checks = 0

async def auto_reconnect():
    # ===== FIX B: + =====
    # : " ".
    # ( stale_message_watchdog )
    # LONG_DISCONNECT_THRESHOLD (1h).
    # 1000 .
    # FIX K: QX_HOST
    global CLIENT, CONNECTION_ALIVE, RECONNECTING, LAST_CONNECTION_DROP_TIME, QX_HOST
    consecutive_failures = 0
    while True:
        await asyncio.sleep(RECONNECT_CHECK_INTERVAL)
        # ===== FIX B: ( ) =====
        # stale_message_watchdog
        # (on_close on_error connect_quotex ).
        # .
        if not CONNECTION_ALIVE and LAST_CONNECTION_DROP_TIME == 0:
            LAST_CONNECTION_DROP_TIME = time.time()
            logmsg(f"[reconnect] connection loss detected at {LAST_CONNECTION_DROP_TIME:.0f}; "
                   f"will re-fetch all candles if outage exceeds {LONG_DISCONNECT_THRESHOLD}s")
        if CONNECTION_ALIVE or RECONNECTING:
            consecutive_failures = 0
            continue
        RECONNECTING = True
        try:
            email = session_manager.get_saved_email()
            # Prefer the in-memory PASSWORD global (always set after the first
            # successful connect_quotex call). If that's not available yet, fall
            # back to the password persisted in app_data.db so auto-reconnect can
            # still work after a fresh process start.
            password_for_reconnect = PASSWORD or session_manager.get_saved_password()
            # ===== FIX: try RECONNECTING =====
            # : `if not (email and PASSWORD): continue`
            # => try RECONNECTING = True =>
            # => !
            if not (email and password_for_reconnect):
                # RECONNECTING
                RECONNECTING = False
                continue
            delay = 0 if consecutive_failures == 0 else min(RECONNECT_BASE_DELAY * (2 ** (consecutive_failures - 1)), RECONNECT_MAX_DELAY)
            if delay > 0:
                logmsg(f"[reconnect] connection lost, retrying in {delay:.0f}s...")
                await asyncio.sleep(delay)
            else:
                logmsg("[reconnect] connection lost, reconnecting immediately...")
            # FIX K:
            _saved_host = session_manager.get_saved_host()
            if _saved_host and QX_HOST != _saved_host:
                QX_HOST = _saved_host
                logmsg(f"[reconnect] using saved host: {QX_HOST}")
            success = await connect_quotex(email, password_for_reconnect, force_fresh=True, max_attempts=1)
            if success:
                logmsg("[reconnect] reconnect successful, restarting streams...")
                consecutive_failures = 0
                # ===== FIX 7: auto-throttle ( ) =====
                # ===== FIX: last_message_at =====
                # stale_watchdog .
                if CLIENT is not None and CLIENT.api is not None:
                    CLIENT.api.last_message_at = time.time()
                # ===== FIX B: =====
                drop_duration = 0.0
                should_refetch_all = False
                if LAST_CONNECTION_DROP_TIME > 0:
                    drop_duration = time.time() - LAST_CONNECTION_DROP_TIME
                    if drop_duration > LONG_DISCONNECT_THRESHOLD:
                        should_refetch_all = True
                        logmsg(
                            f"[reconnect] LONG disconnect detected: {drop_duration/60:.1f} min "
                            f"> {LONG_DISCONNECT_THRESHOLD/60:.0f} min threshold — "
                            f"will re-fetch ALL candles from scratch"
                        )

                LAST_CONNECTION_DROP_TIME = 0
                if should_refetch_all:
                    # streams
                    try:
                        asyncio.run_coroutine_threadsafe(refetch_all_candles(), ASYNC_LOOP)
                    except Exception as e:
                        logmsg(f"[reconnect] failed to schedule full re-fetch: {e}")
                        # fallback: streams
                        for asset in ALL_STREAMING_ASSETS:
                            try:
                                if asset.stream_task and not asset.stream_task.done():
                                    asset.stream_task.cancel()
                                asset.streaming = False
                                asset.stream_task = asyncio.run_coroutine_threadsafe(subsecond_stream(asset) if IS_SUBSECOND else realtime_stream(asset), ASYNC_LOOP)
                                await asyncio.sleep(0.05)
                            except Exception as e2:
                                logmsg(f"[reconnect] failed to restart stream for {asset.symbol}: {e2}")
                else:
                    # (< ) — streams
                    if drop_duration > 0:
                        logmsg(f"[reconnect] short disconnect ({drop_duration:.1f}s) — restarting streams only")
                    for asset in ALL_STREAMING_ASSETS:
                        try:
                            if asset.stream_task and not asset.stream_task.done():
                                asset.stream_task.cancel()
                            asset.streaming = False
                            asset.stream_task = asyncio.run_coroutine_threadsafe(subsecond_stream(asset) if IS_SUBSECOND else realtime_stream(asset), ASYNC_LOOP)
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            logmsg(f"[reconnect] failed to restart stream for {asset.symbol}: {e}")
            else:
                consecutive_failures += 1
        except Exception as e:
            logmsg(f"[reconnect] unexpected error: {e}")
            consecutive_failures += 1
        finally:
            RECONNECTING = False

def _make_progress_printer(symbol: str, start_ts: float, idx: int, total: int):
    display = pretty_asset_name(symbol)
    def on_progress(current, total, label=None, worker_label=None):
        total = total or FETCH_DURATION_SECONDS
        pct = min(current / total, 1.0) if total > 0 else 0
        bar_len = 20
        filled = int(bar_len * pct)
        bar = "#" * filled + "-" * (bar_len - filled)
        elapsed = time.time() - start_ts
        print(f"\r  {display:<25} [{bar}] {pct*100:4.1f}% {elapsed:>2.0f}s", end="", flush=True)
    return on_progress

# ===== BROWSER-STYLE HISTORY (v15) =====
HISTORY_METHOD = "browser"           # "browser" = الجديدة ، "legacy" = الأصلية
HIST_DEBUG = False                   # True = يطبع مفاتيح كل رد (للتشخيص)
SUBSECOND_HISTORY_SECONDS = 3600     # ساعة كاملة للفريمات الفرعية
BROWSER_CHUNK_1S = 3000              # offset المتصفح عند period=1
BROWSER_CHUNK_MIN_CANDLES = 60       # M1 => 60*60 = 3600 مثل المتصفح

def _use_browser_history() -> bool:
    return str(HISTORY_METHOD).lower() == "browser"

def _hist_rows(raw, period):
    keys = ("history", "candles", "data") if period == 1 else ("candles", "data", "history")
    for k in keys:
        v = raw.get(k)
        if isinstance(v, list) and v:
            return v
    return []

def _rows_to_points(rows):
    pts = []
    for c in rows:
        try:
            if isinstance(c, dict):
                t = float(c.get('time', c.get('timestamp', 0)))
                if c.get('price') is not None:
                    o = h = l = cl = float(c['price'])
                else:
                    o = float(c.get('open', 0) or 0)
                    h = float(c.get('high', c.get('max', 0)) or 0)
                    l = float(c.get('low', c.get('min', 0)) or 0)
                    cl = float(c.get('close', 0) or 0)
            elif isinstance(c, (list, tuple)) and len(c) >= 5:
                t, o, cl, h, l = float(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])
            elif isinstance(c, (list, tuple)) and len(c) >= 2:
                t = float(c[0])
                o = h = l = cl = float(c[1])
            else:
                continue
            if t > 0 and o > 0 and h > 0 and l > 0 and cl > 0:
                pts.append((t, o, h, l, cl))
        except (TypeError, ValueError):
            continue
    return pts

def _bucketize(points, bucket):
    out = OrderedDict()
    for t, o, h, l, c in sorted(points, key=lambda x: x[0]):
        b = int(t) // bucket * bucket
        e = out.get(b)
        if e is None:
            out[b] = {'time': b, 'open': o, 'high': h, 'low': l, 'close': c, 'volume': 1}
        else:
            e['high'] = max(e['high'], h)
            e['low'] = min(e['low'], l)
            e['close'] = c
            e['volume'] += 1
    return list(out.values())

async def browser_style_history(api_symbol, period, seconds_back, chunk=None, timeout=15, progress=None):
    if CLIENT is None or CLIENT.api is None:
        return []
    api = CLIENT.api
    if chunk is None:
        chunk = BROWSER_CHUNK_1S if period == 1 else period * BROWSER_CHUNK_MIN_CANDLES
    if api.current_asset != api_symbol:
        await CLIENT.start_candles_stream(api_symbol, period)
        await asyncio.sleep(0.3)
    end_ts = time.time()
    target = end_ts - seconds_back
    cursor = end_ts
    step = 0.001 if period == 1 else 1.0
    points = []
    empty = 0
    batches = 0
    while cursor > target:
        if not getattr(api.state, 'check_accepted_connection', False):
            break
        index = next(_request_counter)
        ev_name = f'candles_ready_{api_symbol}_{index}'
        try:
            raw = await CLIENT._fetch_historical_batch(api_symbol, round(cursor, 3), chunk, period, index, timeout)
        except Exception:
            raw = None
        try:
            await api.event_registry.clear_event(ev_name)
        except Exception:
            pass
        pts = _rows_to_points(_hist_rows(raw, period)) if isinstance(raw, dict) else []
        batches += 1
        if HIST_DEBUG:
            ks = list(raw.keys()) if isinstance(raw, dict) else None
            logmsg(f"[hist p={period}] #{batches} cursor={cursor:.3f} keys={ks} points={len(pts)}")
        if not pts:
            empty += 1
            if empty >= 3:
                break
            cursor -= chunk
            await asyncio.sleep(FETCH_BATCH_DELAY)
            continue
        empty = 0
        points.extend(pts)
        oldest = min(p[0] for p in pts)
        cursor = (oldest - step) if oldest < cursor - step else (cursor - chunk)
        if progress:
            try:
                progress(min(end_ts - cursor, seconds_back), seconds_back)
            except Exception:
                pass
        await asyncio.sleep(0.05)
    points = [p for p in points if p[0] >= target]
    return _bucketize(points, 1 if period == 1 else period)

async def _subsecond_history_browser(asset):
    secs_back = int(SUBSECOND_HISTORY_CANDLES * SUBSECOND_SECONDS * 1.05)
    one_s = await browser_style_history(asset.api_symbol, 1, secs_back)
    if not one_s:
        return []
    tz = _get_tz_offset()
    for c in one_s:
        c['time'] += tz
    agg = _aggregate_sub_minute(one_s, SUBSECOND_SECONDS)
    logmsg(f"[subfetch:{asset.symbol}] browser-style: {len(one_s)} 1s -> {len(agg)} {SUBSECOND_LABEL}")
    return agg[-SUBSECOND_HISTORY_CANDLES:]

async def fetch_candles_once(asset: Asset, idx=1, total=1):
    api_name = asset.api_symbol
    display_name = asset.symbol
    candles = []
    # Sub-minute timeframes: fetch period=1 (1s candles) then aggregate locally
    is_sub_minute = PERIOD_SECONDS < 60
    fetch_period = 1 if is_sub_minute else PERIOD_SECONDS
    # ===== v15: أسلوب المتصفح أولاً =====
    if _use_browser_history():
        try:
            on_progress = _make_progress_printer(display_name, time.time(), idx, total)
            _secs_back = int(INITIAL_CANDLES * PERIOD_SECONDS * 1.05) if is_sub_minute else FETCH_DURATION_SECONDS
            candles = await asyncio.wait_for(
                browser_style_history(api_name, fetch_period, _secs_back, progress=on_progress),
                timeout=180,
            ) or []
        except Exception as e:
            print()
            logmsg(f"[fetch:{display_name}] browser-style history failed: {e} -> legacy fallback")
            candles = []
    if not candles and is_sub_minute:
        # Use get_candles (single request) — more reliable for period=1
        # Make staggered requests going backwards in time
        try:
            on_progress = _make_progress_printer(display_name, time.time(), idx, total)
            try: on_progress(FETCH_DURATION_SECONDS, FETCH_DURATION_SECONDS)
            except Exception: pass
            # الهدف: نحاول جلب ما يكفي لـ INITIAL_CANDLES شموع مجمّعة
            # INITIAL_CANDLES * PERIOD_SECONDS = عدد شموع 1s المطلوبة
            # هامش 1.5x (بدل 2x) لتقليل وقت الجلب وبالتالي تقليل الفجوة الزمنية
            # الفجوة الكبيرة = شموع مفقودة أكثر بين التاريخي والبث المباشر
            target_1s_count = int(INITIAL_CANDLES * PERIOD_SECONDS * 1.5)
            chunk_seconds = 500  # ~500 1s candles per request (server limit)
            all_candles_1s = []
            current_end = time.time()
            # زيادة max_chunks من 20 → 40 لضمان جلب كافٍ حتى للأصول البطيئة
            max_chunks = 40
            chunks_done = 0
            # تتبع الـ chunks الفارغة المتتالية (للإيقاف المبكر الذكي)
            empty_chunks_in_a_row = 0
            while len(all_candles_1s) < target_1s_count and chunks_done < max_chunks:
                chunk_end = current_end - (chunks_done * chunk_seconds)
                try:
                    res_chunk = await asyncio.wait_for(
                        CLIENT.get_candles(api_name, chunk_end, chunk_seconds, fetch_period),
                        timeout=20,
                    )
                except Exception as e:
                    if chunks_done == 0:
                        logmsg(f"[fetch:{display_name}] get_candles(period=1) chunk {chunks_done+1} failed: {e}")
                    # retry once on transient error
                    if empty_chunks_in_a_row < 2:
                        empty_chunks_in_a_row += 1
                        await asyncio.sleep(0.2)
                        continue
                    break
                if not res_chunk or len(res_chunk) == 0:
                    empty_chunks_in_a_row += 1
                    # 3 chunks فارغة متتالية = نهاية التاريخ المتاح → توقف
                    if empty_chunks_in_a_row >= 3:
                        break
                    chunks_done += 1
                    continue
                # عتبة إيقاف أقل صرامة (من 50 → 10): بعض الأصول تُرجع 20-30 شمعة فقط في نهاية الأسبوع
                # هذا طبيعي ولا يعني أننا وصلنا لنهاية التاريخ
                if len(res_chunk) < 10 and chunks_done > 0:
                    all_candles_1s.extend(res_chunk)
                    break
                empty_chunks_in_a_row = 0
                all_candles_1s.extend(res_chunk)
                chunks_done += 1
                try:
                    pct = min(len(all_candles_1s) / target_1s_count, 1.0)
                    on_progress(pct * FETCH_DURATION_SECONDS, FETCH_DURATION_SECONDS)
                except Exception:
                    pass
                # delay صغير (0.03 بدل 0.05) لتسريع الجلب
                await asyncio.sleep(0.03)
            # De-duplicate by time
            seen_ts = set()
            unique_1s = []
            for c in all_candles_1s:
                if isinstance(c, dict):
                    ts = int(float(c.get('time', c.get('timestamp', 0))))
                    if ts not in seen_ts:
                        seen_ts.add(ts)
                        unique_1s.append(c)
            candles = unique_1s
            # No log message here — fetch_candles_with_retry prints the final count
        except Exception as e:
            print()
            logmsg(f"[fetch:{display_name}] get_candles(period=1) failed: {e}")
    elif not candles:
        # 60s+ timeframe — use parallel get_historical_candles
        if hasattr(CLIENT, 'get_historical_candles'):
            try:
                on_progress = _make_progress_printer(display_name, time.time(), idx, total)
                res = await asyncio.wait_for(
                    CLIENT.get_historical_candles(
                        api_name,
                        amount_of_seconds=FETCH_DURATION_SECONDS,
                        period=PERIOD_SECONDS,
                        max_workers=FETCH_MAX_WORKERS,
                        progress_callback=on_progress
                    ),
                    timeout=60,
                )
                if res and len(res) > 0: candles = res
            except Exception as e:
                print()
                logmsg(f"[fetch:{display_name}] get_historical_candles failed: {e}")
    if candles:
        formatted = []
        for c in candles:
            if not isinstance(c, dict): continue
            try:
                ts = int(float(c.get("time", c.get("timestamp", 0))))
                # Align to fetch_period (1s for sub-minute, PERIOD_SECONDS otherwise)
                aligned = _align_time(ts, period_seconds=fetch_period)
                o, h, l, cl = float(c.get("open", 0)), float(c.get("high", c.get("max", 0))), float(c.get("low", c.get("min", 0))), float(c.get("close", 0))
                if o > 0 and h > 0 and l > 0 and cl > 0:
                    formatted.append({'time': aligned, 'open': o, 'high': h, 'low': l, 'close': cl, 'volume': random.randint(50, 200)})
            except Exception: continue
        # Aggregate 1s candles into target subsecond timeframe
        if is_sub_minute and formatted:
            formatted = _aggregate_sub_minute(formatted, PERIOD_SECONDS)
        seen, unique = set(), []
        for c in formatted:
            if c['time'] not in seen:
                seen.add(c['time'])
                unique.append(c)
        unique.sort(key=lambda x: x['time'])
        return unique[-INITIAL_CANDLES:]
    return []


# ==============================================================================
# ===== BRIDGE FETCH HELPER (سد فجوة الشموع الأخيرة بين التاريخي والبث) =====
# ==============================================================================
# هذه الدالة المستقلة تُحاكي أسلوب كود 199 الشموع الذي يعمل بدون ثغرات:
#   1) طلب واحد (وليس متوازي) عبر CLIENT.get_candles
#   2) ينتهي عند time.time() الحالي (وليس T0 عند بداية الجلب التاريخي)
#   3) يستقبل آخر BRIDGE_FETCH_CANDLES شموع بما فيها الشمعة الحالية المتشكلة
#   4) يدمجها مع الشموع التاريخية (الشمعة الجسرية تستبدل التاريخية في نفس الفترة)
#
# الأمان:
#   - إذا فشلت لأي سبب، تُرجع existing_candles كما هي دون أي تغيير
#   - لا تُكسر أبداً سير عمل fetch_candles_with_retry
#   - تُحترم BRIDGE_FETCH_ENABLED=False (يمكن تعطيلها فوراً)
#   - تستخدم Semaphore لمنع إرهاق الخادم بطلبات متزامنة
# ==============================================================================
async def _bridge_fetch_recent_candles(asset: Asset, existing_candles: list) -> list:
    """جلب جسري لآخر BRIDGE_FETCH_CANDLES شمعة لسد فجوة 4-5 شموع.

    Args:
        asset: كائن Asset
        existing_candles: قائمة الشموع التاريخية المُجلبَة مسبقاً

    Returns:
        قائمة مدمجة (تاريخي + جسري) مرتبة ومُزالة التكرار، أو existing_candles
        كما هي عند فشل الجلب (الأمان: لا تُكسر العملية أبداً).
    """
    # ── فحوصات الأمان الأولية ─────────────────────────────────────────────
    if not BRIDGE_FETCH_ENABLED:
        return existing_candles
    if not existing_candles or len(existing_candles) == 0:
        return existing_candles
    if CLIENT is None or CLIENT.api is None:
        return existing_candles
    if not getattr(getattr(CLIENT, 'api', None), 'state', None):
        return existing_candles
    if not getattr(CLIENT.api.state, 'check_accepted_connection', False):
        return existing_candles

    # ── إنشاء Semaphore بأمان (lazy init على الـ loop الحالي) ──────────────
    global _BRIDGE_FETCH_SEMAPHORE
    if _BRIDGE_FETCH_SEMAPHORE is None:
        try:
            _BRIDGE_FETCH_SEMAPHORE = asyncio.Semaphore(BRIDGE_FETCH_SEMAPHORE_LIMIT)
        except RuntimeError:
            # الـ loop غير جاهز — تُرجع existing_candles دون كسر العمل
            return existing_candles

    try:
        async with _BRIDGE_FETCH_SEMAPHORE:
            is_sub_minute = PERIOD_SECONDS < 60
            # ✨ السر: استخدم time.time() الحالي (وليس T0 عند بداية الجلب التاريخي)
            bridge_end_time = int(time.time())
            bridge_candles = []

            if is_sub_minute:
                # ── للثواني الفرعية: جلب 1s ثم تجميع محلياً ──────────────────
                # هامش 5x لضمان تغطية كاملة للفجوة الزمنية الكبيرة المحتملة
                # (BRIDGE_FETCH_CANDLES=15 × PERIOD_SECONDS=5 × 5 = 375 ثانية من شموع 1s)
                # هذا يضمن أن الجسر يغطي 75 شمعة 5s — أكثر من كافٍ لأي فجوة
                bridge_1s_offset = BRIDGE_FETCH_CANDLES * PERIOD_SECONDS * 5
                try:
                    bridge_1s = await asyncio.wait_for(
                        CLIENT.get_candles(
                            asset.api_symbol,
                            bridge_end_time,
                            bridge_1s_offset,
                            1  # period=1 (1s candles — مدعوم دائماً من الخادم)
                        ),
                        timeout=BRIDGE_FETCH_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    logmsg(f"[bridge:{asset.symbol}] 1s fetch timeout (non-fatal)")
                    return existing_candles
                except Exception as e:
                    logmsg(f"[bridge:{asset.symbol}] 1s fetch failed (non-fatal): {e}")
                    return existing_candles

                if not bridge_1s:
                    return existing_candles

                # تنسيق شموع 1s
                bridge_formatted = []
                for c in bridge_1s:
                    if not isinstance(c, dict):
                        continue
                    try:
                        ts = int(float(c.get("time", c.get("timestamp", 0))))
                        aligned = _align_time(ts, period_seconds=1)
                        o = float(c.get("open", 0) or 0)
                        h = float(c.get("high", c.get("max", 0)) or 0)
                        l = float(c.get("low", c.get("min", 0)) or 0)
                        cl = float(c.get("close", c.get("c", 0)) or 0)
                        if o > 0 and h > 0 and l > 0 and cl > 0:
                            bridge_formatted.append({
                                'time': aligned, 'open': o, 'high': h,
                                'low': l, 'close': cl, 'volume': 1
                            })
                    except Exception:
                        continue

                # تجميع الـ 1s إلى الإطار الزمني المطلوب (5s/10s/15s/30s)
                bridge_candles = _aggregate_sub_minute(bridge_formatted, PERIOD_SECONDS)
            else:
                # ── لإطارات 60s+: استخدام _fetch_historical_batch مباشرة ──
                # لماذا؟ get_candles تُعيد subscribe للـ WebSocket مما يتعارض مع
                # realtime_stream الذي يعمل في الخلفية. _fetch_historical_batch
                # يستخدم event_name فريد (مع index) ولا يُعيد subscribe.
                # هذا يحل مشكلة "الفجوة عادت في M1+".
                bridge_offset = PERIOD_SECONDS * BRIDGE_FETCH_CANDLES
                # index فريد لتجنب التعارض مع get_historical_candles السابق
                _bridge_index = next(_request_counter)
                try:
                    bridge_raw = await asyncio.wait_for(
                        CLIENT._fetch_historical_batch(
                            asset.api_symbol,
                            bridge_end_time,
                            bridge_offset,
                            PERIOD_SECONDS,
                            _bridge_index,
                            BRIDGE_FETCH_TIMEOUT
                        ),
                        timeout=BRIDGE_FETCH_TIMEOUT + 5,  # هامش 5s إضافي
                    )
                except asyncio.TimeoutError:
                    logmsg(f"[bridge:{asset.symbol}] fetch timeout (non-fatal)")
                    return existing_candles
                except Exception as e:
                    logmsg(f"[bridge:{asset.symbol}] fetch failed (non-fatal): {e}")
                    return existing_candles

                if not bridge_raw:
                    return existing_candles

                # تحويل النتيجة عبر _parse_historical_candles (نفس آلية get_historical_candles)
                try:
                    bridge_parsed = CLIENT._parse_historical_candles(bridge_raw)
                except Exception:
                    bridge_parsed = []

                if not bridge_parsed:
                    return existing_candles

                # تنسيق شموع الجسر
                for c in bridge_parsed:
                    if not isinstance(c, dict):
                        continue
                    try:
                        ts = int(c.get("time", c.get("timestamp", 0)))
                        aligned = _align_time(ts, period_seconds=PERIOD_SECONDS)
                        o = float(c.get("open", 0) or 0)
                        h = float(c.get("high", 0) or 0)
                        l = float(c.get("low", 0) or 0)
                        cl = float(c.get("close", 0) or 0)
                        if o > 0 and h > 0 and l > 0 and cl > 0:
                            bridge_candles.append({
                                'time': aligned, 'open': o, 'high': h,
                                'low': l, 'close': cl, 'volume': 1
                            })
                    except Exception:
                        continue

            if not bridge_candles:
                return existing_candles

            # ── الدمج الآمن: الجسر يستبدل التاريخي في نفس الفترة الزمنية ───
            # (الشموع الجسرية أحدث وأدق، خاصة للشموع الأخيرة غير المكتملة)
            bridge_by_time = {c['time']: c for c in bridge_candles}
            merged_list = []
            for c in existing_candles:
                if c['time'] in bridge_by_time:
                    # استبدل الشمعة التاريخية بالجسرية الأحدث
                    merged_list.append(bridge_by_time.pop(c['time']))
                else:
                    merged_list.append(c)
            # أضف الشموع الجسرية المتبقية (جديدة، غير موجودة في التاريخي)
            merged_list.extend(bridge_by_time.values())

            # ── الترتيب وإزالة التكرار ─────────────────────────────────────
            seen = set()
            unique = []
            for c in sorted(merged_list, key=lambda x: x['time']):
                if c['time'] not in seen:
                    seen.add(c['time'])
                    unique.append(c)

            # قص إلى INITIAL_CANDLES (نفس سلوك الكود الأصلي)
            result = unique[-INITIAL_CANDLES:]

            # تسجيل بسيط (فقط إذا تم إضافة شموع جديدة فعلية)
            added = len(result) - len(existing_candles)
            if added > 0:
                logmsg(f"[bridge:{asset.symbol}] +{added} new candles bridged (total {len(result)})")

            return result

    except Exception as e:
        # ── الأمان النهائي: لا تُكسر الجلب التاريخي أبداً ──────────────────
        try:
            logmsg(f"[bridge:{asset.symbol}] bridge fetch error (non-fatal): {e}")
        except Exception:
            pass
        return existing_candles


async def fetch_candles_with_retry(asset: Asset, max_retries=3, idx=1, total=1):
    # For sub-minute, use lower threshold + 1 retry (server caps ~500-1000 1s candles)
    is_sub_minute = PERIOD_SECONDS < 60
    threshold = 20 if is_sub_minute else MIN_CANDLES_THRESHOLD
    max_retries = 1 if is_sub_minute else MAX_FETCH_RETRIES
    last_error = None
    for attempt in range(1, max_retries + 1):

        if not CONNECTION_ALIVE or CLIENT is None or CLIENT.api is None:
            logmsg(f"[fetch_retry:{asset.symbol}] connection dead before attempt {attempt}; waiting for reconnect...")
            # (timeout 15s)
            try:
                await wait_until(
                    lambda: CONNECTION_ALIVE and CLIENT is not None and CLIENT.api is not None,
                    timeout=15.0,
                    poll_interval=0.5,
                )
            except asyncio.TimeoutError:
                logmsg(f"[fetch_retry:{asset.symbol}] reconnect did not happen within 15s; giving up on this asset")
                return 0, attempt
        try:
            candles = await fetch_candles_once(asset, idx, total)
        except Exception as e:
            candles = []
            last_error = e
            logmsg(f"[fetch_retry:{asset.symbol}] attempt {attempt}/{max_retries} raised: {e}")
        # ===== BRIDGE FETCH: سد فجوة 4-5 شموع بين التاريخي والبث المباشر =====
        # استدعاء آمن: أي خطأ يُرجع candles كما هي دون كسر العملية.
        # يُجرّب آخر BRIDGE_FETCH_CANDLES شمعة تنتهي عند time.time() الحالي.
        if candles and len(candles) > 0:
            try:
                candles = await _bridge_fetch_recent_candles(asset, candles)
            except Exception as bridge_err:
                # الأمان التام: لا تُكسر الجلب التاريخي أبداً
                logmsg(f"[fetch_retry:{asset.symbol}] bridge skipped (non-fatal): {bridge_err}")
        if len(candles) >= threshold:
            # Merge historical candles with any live ticks collected during fetch
            # (realtime_stream may have appended live candles to asset.candles)
            live_candles = []
            if asset.candles:
                hist_times = set(c['time'] for c in candles)
                for c in asset.candles:
                    if c['time'] not in hist_times and c['time'] > (candles[-1]['time'] if candles else 0):
                        live_candles.append(c)
            # (تعديل 9) دمج إضافي فوق الأرضية الثابتة — لا شمعة مُسلَّمة تختفي هنا
            asset.candles = MT4_WRITER.stabilize(asset.symbol, candles + live_candles)
            if asset.candles: asset.price = asset.candles[-1]['close']
            asset.write(force=True)
            # Update MT4_WRITER.sync_state with the full historical + live candles
            # so update_candle doesn't overwrite HST with only the few live candles
            MT4_WRITER.sync_state[asset.symbol] = {
                'period': PERIOD,
                'candles': list(asset.candles),
                'last_write': 0,
                'last_price': float(asset.candles[-1]['close']) if asset.candles else 0.0,
            }
            return len(asset.candles), attempt

        # Merge even for low counts
        live_candles = []
        if asset.candles:
            hist_times = set(c['time'] for c in candles)
            for c in asset.candles:
                if c['time'] not in hist_times and c['time'] > (candles[-1]['time'] if candles else 0):
                    live_candles.append(c)
        # (تعديل 9) دمج إضافي فوق الأرضية الثابتة — لا شمعة مُسلَّمة تختفي هنا
        asset.candles = MT4_WRITER.stabilize(asset.symbol, candles + live_candles)
        if asset.candles: asset.price = asset.candles[-1]['close']
        asset.write(force=True)
        # Update sync_state even for low counts
        if asset.candles:
            MT4_WRITER.sync_state[asset.symbol] = {
                'period': PERIOD,
                'candles': list(asset.candles),
                'last_write': 0,
                'last_price': float(asset.candles[-1]['close']) if asset.candles else 0.0,
            }
        # ===== FIX 3: exponential backoff retries =====
        if attempt < max_retries:
            delay = min(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)), RETRY_BACKOFF_MAX)
            logmsg(f"[fetch_retry:{asset.symbol}] attempt {attempt}/{max_retries} got {len(candles)} candles; retry in {delay:.1f}s")
            await asyncio.sleep(delay)
    if last_error:
        logmsg(f"[fetch_retry:{asset.symbol}] all {max_retries} attempts failed. Last error: {last_error}")
    return len(candles), max_retries


# ==============================================================================
# SUBSECOND STREAM — separate system for 5s/10s/15s/30s timeframes
# Does NOT use fetch_candles_with_retry, MT4_WRITER.update_candle, or mt4_gap_filler.
# Aggregates realtime ticks into subsecond candles and writes HST directly.
# ==============================================================================

def _subsecond_align(ts: int) -> int:
    """Align timestamp to subsecond window boundary (with TZ offset for consistency)."""
    tz = _get_tz_offset()
    return ((int(ts) + tz) // SUBSECOND_SECONDS) * SUBSECOND_SECONDS

def _subsecond_write_hst(asset_symbol: str, candles: list):
    """(تم نزع MT4) — كانت تكتب ملف HST للفريمات الفرعية؛ أصبحت حلقة فارغة
    عمداً حتى تبقى كل نقاط الاستدعاء داخل مسار البث المباشر كما هي دون تعديل."""
    return


async def fetch_subsecond_history(asset: Asset, idx=1, total=1):
    """Fetch historical subsecond candles by fetching 1s candles and aggregating.

    Strategy (per Quotex server limitations):
    1. Use `get_candles` (single request, NOT parallel get_historical_candles)
       with period=1 — server returns ~500 1s candles per request
    2. Make multiple staggered requests going backwards in time
    3. Stop when server returns 3 empty chunks in a row (hit history limit)
    4. Aggregate 1s candles locally into 5s/10s/15s/30s

    Uses a semaphore (3 concurrent) to avoid overwhelming server.
    """
    if CLIENT is None or not hasattr(CLIENT, 'get_candles'):
        logmsg(f"[subfetch:{asset.symbol}] CLIENT not ready")
        return []
    # ===== v15: أسلوب المتصفح أولاً =====
    if _use_browser_history():
        try:
            _res = await _subsecond_history_browser(asset)
            if _res:
                return _res
        except Exception as _e:
            logmsg(f"[subfetch:{asset.symbol}] browser-style failed: {_e}")
        logmsg(f"[subfetch:{asset.symbol}] falling back to legacy method")
    api_name = asset.api_symbol
    display_name = asset.symbol
    # Target: SUBSECOND_HISTORY_CANDLES aggregated candles
    # = SUBSECOND_HISTORY_CANDLES * SUBSECOND_SECONDS 1s candles
    # هامش 1.5x (بدل 2x) لتقليل وقت الجلب → تقليل الفجوة الزمنية بين التاريخي والبث
    target_1s_count = int(SUBSECOND_HISTORY_CANDLES * SUBSECOND_SECONDS * 1.5)
    chunk_seconds = 500  # ~500 1s candles per request (server limit)
    all_candles_1s = []
    current_end = time.time()
    # زيادة max_chunks من 20 → 40
    max_chunks = 40
    chunks_done = 0
    empty_chunks_in_a_row = 0
    # Semaphore (3 concurrent fetches, not 41) — created lazily on the running loop
    global _SUBSECOND_FETCH_SEMAPHORE
    if _SUBSECOND_FETCH_SEMAPHORE is None:
        _SUBSECOND_FETCH_SEMAPHORE = asyncio.Semaphore(3)
    async with _SUBSECOND_FETCH_SEMAPHORE:
        logmsg(f"[subfetch:{display_name}] start: get_candles(period=1), target ~{target_1s_count} 1s candles")
        try:
            while len(all_candles_1s) < target_1s_count and chunks_done < max_chunks:
                if not CLIENT or not CLIENT.api or not getattr(CLIENT.api.state, 'check_accepted_connection', False):
                    logmsg(f"[subfetch:{display_name}] connection lost during fetch")
                    break
                chunk_end = current_end - (chunks_done * chunk_seconds)
                try:
                    res_chunk = await asyncio.wait_for(
                        CLIENT.get_candles(api_name, chunk_end, chunk_seconds, 1),
                        timeout=20,
                    )
                except Exception as e:
                    if chunks_done == 0:
                        logmsg(f"[subfetch:{display_name}] get_candles(period=1) chunk {chunks_done+1} failed: {e}")
                    # retry on transient error
                    if empty_chunks_in_a_row < 2:
                        empty_chunks_in_a_row += 1
                        await asyncio.sleep(0.2)
                        continue
                    break
                if not res_chunk or len(res_chunk) == 0:
                    empty_chunks_in_a_row += 1
                    # 3 chunks فارغة متتالية = نهاية التاريخ المتاح
                    if empty_chunks_in_a_row >= 3:
                        break
                    chunks_done += 1
                    continue
                # عتبة إيقاف أقل صرامة (من 50 → 10)
                if len(res_chunk) < 10 and chunks_done > 0:
                    all_candles_1s.extend(res_chunk)
                    break
                empty_chunks_in_a_row = 0
                all_candles_1s.extend(res_chunk)
                chunks_done += 1
                # Small delay between chunks to avoid server rate limiting
                await asyncio.sleep(0.03)
        except Exception as e:
            logmsg(f"[subfetch:{display_name}] FAILED: {e}")
            return []
    # De-duplicate by time (chunks may overlap)
    seen_ts = set()
    unique_1s = []
    for c in all_candles_1s:
        if isinstance(c, dict):
            ts = int(float(c.get('time', c.get('timestamp', 0))))
            if ts not in seen_ts:
                seen_ts.add(ts)
                unique_1s.append(c)
    logmsg(f"[subfetch:{display_name}] got {len(unique_1s)} 1s candles from {chunks_done} chunks, aggregating to {SUBSECOND_LABEL}")
    # Format 1s candles with TZ offset
    formatted_1s = []
    for c in unique_1s:
        try:
            ts = int(float(c.get("time", c.get("timestamp", 0))))
            tz = _get_tz_offset()
            aligned = ((ts + tz) // 1) * 1  # align to 1s boundary with TZ
            o = float(c.get("open", 0) or 0)
            h = float(c.get("high", c.get("max", 0)) or 0)
            l = float(c.get("low", c.get("min", 0)) or 0)
            cl = float(c.get("close", 0) or 0)
            v = int(c.get("volume", c.get("vol", 0)) or 0)
            if o > 0 and h > 0 and l > 0 and cl > 0:
                formatted_1s.append({
                    'time': aligned, 'open': o, 'high': h, 'low': l,
                    'close': cl, 'volume': v
                })
        except Exception:
            continue
    formatted_1s.sort(key=lambda x: x['time'])
    # Aggregate 1s candles into target subsecond timeframe
    aggregated = _aggregate_sub_minute(formatted_1s, SUBSECOND_SECONDS)
    # Limit to SUBSECOND_HISTORY_CANDLES
    result = aggregated[-SUBSECOND_HISTORY_CANDLES:]
    logmsg(f"[subfetch:{display_name}] DONE: {len(unique_1s)} 1s -> {len(result)} {SUBSECOND_LABEL} candles")
    return result

async def subsecond_stream(asset: Asset):
    """Dedicated stream for subsecond timeframes (5s/10s/15s/30s).

    - Subscribes to realtime ticks (same WS subscription as normal)
    - Polls realtime_candles[asset] for tick data
    - Aggregates ticks into subsecond candle windows
    - Writes HST directly (bypasses MT4_WRITER.sync_state entirely)
    - Keeps last SUBSECOND_MAX_CANDLES (200) candles
    - NO gap filler, NO history fetch
    """
    internal = asset.api_symbol
    try:
        try:
            await wait_until(
                lambda: not (CONNECTION_ALIVE and CLIENT is None),
                timeout=10.0,
                poll_interval=0.2,
            )
        except asyncio.TimeoutError:
            pass
        if not CONNECTION_ALIVE or CLIENT is None:
            asset.streaming = False
            return
        # Subscribe to realtime 1-second ticks (period=1, always supported)
        # We aggregate locally into subsecond windows
        await CLIENT.start_candles_stream(internal, 1)
        await asyncio.sleep(0.5)
        asset.streaming = True
        asset.candles = []
        # Write empty HST immediately so MT4 can open the chart
        _subsecond_write_hst(asset.symbol, [{'time': _subsecond_align(int(time.time())), 'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0}])
        last_write = time.time()
        # Fetch historical subsecond candles in the BACKGROUND using create_task
        # (run_coroutine_threadsafe doesn't work from inside the same loop)
        display_name = pretty_asset_name(internal)
        async def _background_history_fetch():
            try:
                historical = await asyncio.wait_for(fetch_subsecond_history(asset), timeout=60)
                if historical and len(historical) > 0:
                    # Merge: keep historical candles older than any tick-collected candles
                    if asset.candles:
                        oldest_tick_time = asset.candles[0]['time']
                        historical_to_keep = [c for c in historical if c['time'] < oldest_tick_time]
                        asset.candles = historical_to_keep + asset.candles
                    else:
                        asset.candles = historical
                    asset.candles = asset.candles[-SUBSECOND_MAX_CANDLES:]
                    if asset.candles and asset.price == 0:
                        asset.price = asset.candles[-1]['close']
                    # Write HST with merged data
                    _subsecond_write_hst(asset.symbol, asset.candles)
                    logmsg(f"[substream:{internal}] {len(historical)} history candles loaded + ticks active")
                else:
                    logmsg(f"[substream:{internal}] no history (ticks only)")
            except asyncio.TimeoutError:
                logmsg(f"[substream:{internal}] history fetch timeout (ticks only)")
            except Exception as e:
                logmsg(f"[substream:{internal}] background history fetch failed: {e}")
        # Schedule on the same loop — keep a strong reference to avoid GC
        _task = asyncio.create_task(_background_history_fetch())
        # Store on asset to prevent garbage collection
        if not hasattr(asset, '_bg_tasks'):
            asset._bg_tasks = set()
        asset._bg_tasks.add(_task)
        _task.add_done_callback(asset._bg_tasks.discard)
    except Exception as e:
        logmsg(f"[substream:{internal}] failed to start: {e}")
        asset.streaming = False
        return
    consecutive_errors = 0
    while CONNECTION_ALIVE:
        try:
            if CLIENT is None or not getattr(getattr(CLIENT, 'api', None), 'state', None) or getattr(CLIENT.api.state, 'status', None) != 1:
                await asyncio.sleep(0.2)
                continue
            candle = None
            if hasattr(CLIENT, 'api') and CLIENT.api:
                candle = CLIENT.api.realtime_candles.get(internal)
            if candle:
                consecutive_errors = 0
                if isinstance(candle, list) and len(candle) >= 3:
                    ts, price = int(candle[1]), float(candle[2])
                elif isinstance(candle, dict):
                    ts = int(candle.get("time", candle.get("timestamp", time.time())))
                    price = float(candle.get("price", candle.get("close", 0)))
                else:
                    await asyncio.sleep(STREAM_POLL_INTERVAL)
                    continue
                if price > 0 and ts > 0:
                    # Align to subsecond window
                    window_start = _subsecond_align(ts)
                    if asset.candles:
                        last_candle = asset.candles[-1]
                        if window_start < last_candle['time']:
                            # stale tick — ignore
                            pass
                        elif window_start == last_candle['time']:
                            # update current candle
                            last_candle['high'] = max(last_candle['high'], price)
                            last_candle['low'] = min(last_candle['low'], price)
                            last_candle['close'] = price
                            last_candle['volume'] += 1
                        else:
                            # new window — append
                            asset.candles.append({
                                'time': window_start, 'open': price, 'high': price,
                                'low': price, 'close': price, 'volume': 1
                            })
                    else:
                        asset.candles.append({
                            'time': window_start, 'open': price, 'high': price,
                            'low': price, 'close': price, 'volume': 1
                        })
                    # keep last SUBSECOND_MAX_CANDLES
                    if len(asset.candles) > SUBSECOND_MAX_CANDLES:
                        asset.candles = asset.candles[-SUBSECOND_MAX_CANDLES:]
                    # update price display
                    if asset.price != price:
                        asset.previous_price = asset.price
                    asset.price = price
                    asset.updates += 1
                    asset.last_update_time = time.time()
                    # تحديث السعر كل تيكس — أوقظ حلقة الجدول الحي فوراً
                    TICK_UPDATE_EVENT.set()
                    # write HST every 0.5s
                    now = time.time()
                    if now - last_write > 0.5:
                        _subsecond_write_hst(asset.symbol, asset.candles)
                        last_write = now
            await asyncio.sleep(STREAM_POLL_INTERVAL)
        except asyncio.CancelledError:
            asset.streaming = False
            raise
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            consecutive_errors += 1
            if consecutive_errors >= 10:
                logmsg(f"[substream:{internal}] too many consecutive errors, stopping")
                asset.streaming = False
                return
            await asyncio.sleep(0.2)
    asset.streaming = False


async def realtime_stream(asset: Asset):
    internal = asset.api_symbol
    try:
        # ===== EVENT-DRIVEN FIX =====
        # :
        # while CONNECTION_ALIVE and CLIENT is None: await asyncio.sleep(0.5)
        # timeout 200ms .
        try:
            await wait_until(
                lambda: not (CONNECTION_ALIVE and CLIENT is None),
                timeout=10.0,
                poll_interval=0.2,
            )
        except asyncio.TimeoutError:
            pass
        if not CONNECTION_ALIVE or CLIENT is None:
            asset.streaming = False
            return
        # For sub-minute timeframes, subscribe to period=1 (1s candles, always supported)
        # _align_time then buckets ticks into PERIOD_SECONDS (5s, 10s, 15s, 30s)
        stream_period = 1 if PERIOD_SECONDS < 60 else PERIOD_SECONDS
        await CLIENT.start_candles_stream(internal, stream_period)
        await asyncio.sleep(0.3)  # reduced from 1.5s to minimize gap with historical fetch
        asset.streaming = True
        # ===== FIX 12: MT4_WRITER.sync_state 1000 =====
        # : MT4_WRITER.update_candle sync_state
        # => ~19 (1KB) 1000 (44KB)
        # : asset.candles (1000 ) sync_state[derived_symbol]
        # realtime_stream update_candle
        # sync_state 1000 .
        if asset.candles and asset.symbol not in MT4_WRITER.sync_state:
            MT4_WRITER.sync_state[asset.symbol] = {
                'period': PERIOD,
                'candles': list(asset.candles),  # 1000
                'last_write': 0,
                'last_price': float(asset.candles[-1]['close']) if asset.candles else 0.0,
            }
    except Exception as e:
        logmsg(f"[stream:{internal}] failed to start stream: {e}")
        asset.streaming = False
        return
    consecutive_errors = 0
    while CONNECTION_ALIVE:
        try:
            if CLIENT is None or not getattr(getattr(CLIENT, 'api', None), 'state', None) or getattr(CLIENT.api.state, 'status', None) != 1:
                # ===== EVENT-DRIVEN FIX =====
                # await asyncio.sleep(1) — polling (200ms)
                await asyncio.sleep(0.2)
                continue
            candle = None
            if not candle and hasattr(CLIENT, 'api') and CLIENT.api:
                candle = CLIENT.api.realtime_candles.get(internal)
            if candle:
                consecutive_errors = 0
                if isinstance(candle, list) and len(candle) >= 3:
                    ts, price = int(candle[1]), float(candle[2])
                elif isinstance(candle, dict):
                    ts = int(candle.get("time", candle.get("timestamp", time.time())))
                    price = float(candle.get("price", candle.get("close", 0)))
                else:
                    await asyncio.sleep(STREAM_POLL_INTERVAL)
                    continue
                if price > 0 and ts > 0:
                    # ===== FIX 10: MT4_WRITER.update_candle asset.mark_dirty =====
                    # HST MT4Writer.write_to_file ( )
                    # MT4_WRITER.update_candle sync_state[derived_symbol] + HST 200ms
                    try:
                        MT4_WRITER.update_candle(asset.symbol, PERIOD, {'time': ts, 'price': price})
                    except Exception:
                        pass
                    # asset.candles ( )
                    # FIX L5: _align_time ( tz_offset) asset.candles
                    # sync_state HST. (ts // 60 * 60)
                    # tz_offset => 1+ HST.
                    aligned_time = _align_time(ts)
                    # FIX STALE-CANDLE v2: same logic as update_candle
                    if asset.candles:
                        last_candle = asset.candles[-1]
                        if aligned_time < last_candle['time']:
                            pass  # tick for closed minute - ignore
                        elif last_candle['time'] == aligned_time:
                            last_candle['high'] = max(last_candle['high'], price)
                            last_candle['low'] = min(last_candle['low'], price)
                            last_candle['close'] = price
                        else:
                            # new candle - fill gap with flat candles first
                            period_secs = PERIOD_SECONDS if PERIOD_SECONDS > 0 else 60
                            gap = (aligned_time - last_candle['time']) // period_secs
                            if gap > 1:
                                last_close = last_candle['close']
                                fill_time = last_candle['time'] + period_secs
                                while fill_time < aligned_time:
                                    asset.candles.append({
                                        'time': fill_time,
                                        'open': last_close, 'high': last_close,
                                        'low': last_close, 'close': last_close, 'volume': 0,
                                    })
                                    fill_time += period_secs
                            asset.candles.append({'time': aligned_time, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': 1})
                    else:
                        asset.candles.append({'time': aligned_time, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': 1})
                    if len(asset.candles) > INITIAL_CANDLES: asset.candles = asset.candles[-INITIAL_CANDLES:]
                    # FIX G: ( )
                    if asset.price != price:
                        asset.previous_price = asset.price
                    asset.price = price
                    asset.updates += 1
                    asset.last_update_time = time.time()
                    # تحديث السعر كل تيكس — أوقظ حلقة الجدول الحي فوراً
                    TICK_UPDATE_EVENT.set()
                    # asset.mark_dirty() — MT4_WRITER.update_candle
            await asyncio.sleep(STREAM_POLL_INTERVAL)
        except asyncio.CancelledError:
            asset.streaming = False
            raise
        except asyncio.TimeoutError: continue
        except Exception as e:
            consecutive_errors += 1
            if consecutive_errors >= 10:
                logmsg(f"[stream:{internal}] too many consecutive errors, stopping stream")
                asset.streaming = False
                return
            # ===== EVENT-DRIVEN FIX =====
            # await asyncio.sleep(1) — polling (200ms)
            await asyncio.sleep(0.2)
    asset.streaming = False


# ===== FIX B: (> ) =====
# : " ".
# auto_reconnect
# LONG_DISCONNECT_THRESHOLD. 1000
# HST streams —
# .
async def refetch_all_candles():
    """ .

 :
 1. streams caches 
 2. sync_state MT4_WRITER ( HST)
 3. fetch_candles_with_retry ( 1000 )
 4. realtime_stream 
 5. mt4_gap_filler 
 """
    if not ALL_STREAMING_ASSETS:
        logmsg("[refetch_all] no assets to re-fetch")
        return
    print(f"\n{Colors.CYAN}{'═'*60}{Colors.RESET}")
    print(f"{Colors.BOLD}  FULL RE-FETCH: long disconnect detected — refreshing all candles{Colors.RESET}")
    print(f"  • Assets: {len(ALL_STREAMING_ASSETS)}")
    print(f"  • Target per asset: {INITIAL_CANDLES} candles")
    print(f"{Colors.CYAN}{'═'*60}{Colors.RESET}")
    # 1) streams
    for asset in ALL_STREAMING_ASSETS:
        try:
            if asset.stream_task and not asset.stream_task.done():
                asset.stream_task.cancel()
        except Exception:
            pass
        asset.streaming = False
        asset.stream_task = None
        # (تعديل 9) لا نمسح الشموع المعروضة — الدمج الثابت سيحافظ على كل
        # شمعة مُسلَّمة بعد السد، والجديد يُضاف فوقها فقط.
        asset.last_saved_close = 0.0
        asset.last_saved_candle_time = 0
        asset.last_saved_candle_count = 0
        asset.dirty = True
        asset._gap_filled = False
        asset.updates = 0
        asset.last_update_time = 0
        # sync_state MT4_WRITER
        if asset.symbol in MT4_WRITER.sync_state:
            try: del MT4_WRITER.sync_state[asset.symbol]
            except Exception: pass
        if asset.symbol in MT4_WRITER._gap_filled:
            MT4_WRITER._gap_filled[asset.symbol] = False
        # (تعديل 9) نصفر علامة التجميد فقط — الشموع المحفوظة تبقى كأرضية
        # ثابتة ولا تُمسح، حتى لا تختفي شموع مُسلَّمة بعد إعادة الجلب، ويستطيع
        # السد ملء أي فجوات جديدة فقط.
        try:
            MT4_WRITER.candle_store.clear_gap_state(asset.symbol)
        except Exception:
            pass
    # 2)
    total_loaded = 0
    total_assets = len(ALL_STREAMING_ASSETS)
    consecutive_disconnects = 0
    for idx, asset in enumerate(ALL_STREAMING_ASSETS, 1):

        if not CONNECTION_ALIVE or CLIENT is None or CLIENT.api is None:
            logmsg(f"[refetch_all] connection lost again at asset {idx}/{total_assets}; aborting re-fetch")
            break
        display = pretty_asset_name(asset.api_symbol)
        start_time = time.time()
        if IS_SUBSECOND:
            # Subsecond mode: no history to re-fetch, just restart stream
            print(f"\r{Colors.CLEAR_LINE}  {Colors.CYAN}{display:<25}{Colors.RESET} {Colors.CYAN}subsecond — no history to refetch{Colors.RESET}")
        else:
            try:
                candles_count, attempts = await fetch_candles_with_retry(
                    asset, max_retries=MAX_FETCH_RETRIES, idx=idx, total=total_assets
                )
                elapsed = time.time() - start_time
                if candles_count >= MIN_CANDLES_THRESHOLD:
                    total_loaded += candles_count
                    consecutive_disconnects = 0
                    attempts_str = "" if attempts == 1 else f" ({attempts} attempts)"
                    print(f"\r{Colors.CLEAR_LINE}  {Colors.CYAN}{display:<25}{Colors.RESET} {Colors.GREEN}{candles_count} candles in {elapsed:.1f}s{attempts_str}{Colors.RESET}")
                else:
                    print(f"\r{Colors.CLEAR_LINE}  {Colors.CYAN}{display:<25}{Colors.RESET} {Colors.RED}Only {candles_count} candles{Colors.RESET}")
                # ===== v16: حفظ JSON بعد إعادة جلب شموع هذه العملة =====
                if candles_count > 0:
                    _saved = _save_candles_json(asset, source_label="refetch")
                    if _saved:
                        print(f"  {Colors.DIM}→ Saved: {_saved.name} ({candles_count} candles){Colors.RESET}")
            except Exception as e:
                print(f"\r{Colors.CLEAR_LINE}  {Colors.CYAN}{display:<25}{Colors.RESET} {Colors.RED}Error: {str(e)[:30]}{Colors.RESET}")
                if "Connection" in str(e) or "closed" in str(e).lower():
                    consecutive_disconnects += 1
                    if consecutive_disconnects >= MAX_RECONNECTS_DURING_FETCH:
                        logmsg(f"[refetch_all] too many disconnects ({consecutive_disconnects}); aborting")
                        break
        # 3) restart stream
        try:
            asset.stream_task = asyncio.run_coroutine_threadsafe(subsecond_stream(asset) if IS_SUBSECOND else realtime_stream(asset), ASYNC_LOOP)
        except Exception as e:
            logmsg(f"[refetch_all] failed to restart stream for {asset.symbol}: {e}")
        await asyncio.sleep(FETCH_ASSET_DELAY)
    # 4)
    if CONNECTION_ALIVE and CLIENT is not None and CLIENT.api is not None:
        try:
            asyncio.run_coroutine_threadsafe(mt4_gap_filler(), ASYNC_LOOP)
        except Exception as e:
            logmsg(f"[refetch_all] failed to schedule gap filler: {e}")
    print(f"{Colors.CYAN}{'═'*60}{Colors.RESET}")
    print(f"{Colors.GREEN}  Full re-fetch complete: {total_loaded} candles across {total_assets} assets{Colors.RESET}")
    print(f"{Colors.CYAN}{'═'*60}{Colors.RESET}\n")

# ==============================================================================
# SECTION 9: MT4 SYNC ENGINE ( — )
# ==============================================================================

# ------------------------------------------------------------------------------
# CandleStore: SQLite-backed persistent candle storage.
# ------------------------------------------------------------------------------
# Why SQLite instead of JSON files:
#   - JSON is slow: full file rewrite on every save, full file load on every
#     read, no indexing. For ~1000 candles per asset across many assets this
#     wastes I/O and risks corruption if the process is killed mid-write.
#   - SQLite is transactional, atomic, indexed by (symbol, time), and
#     supports efficient range queries ("give me all candles for symbol X
#     from time A to time B") without loading the whole table into memory.
#   - Concurrent reads + a single writer are safe out of the box.
#
# Schema:
#   candles(symbol TEXT, time INTEGER, open REAL, high REAL, low REAL,
#           close REAL, volume INTEGER, PRIMARY KEY(symbol, time))
#   gap_state(symbol TEXT PRIMARY KEY, filled INTEGER, last_fill_at REAL)
#       - 'filled' is 0/1 and tracks whether fill_gap_once has already
#         succeeded for this asset. Once it is 1, MT4Writer.fill_gap_once
#         stops touching closed candles for this asset entirely.
# ------------------------------------------------------------------------------
import sqlite3 as _sqlite3
import threading as _threading

class CandleStore:
    """Thread-safe SQLite-backed candle persistence layer.

    All public methods are safe to call from any thread. A single write
    connection (guarded by a lock) serializes writes; reads use a short-lived
    read-only connection per call.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._write_lock = _threading.Lock()
        self._write_conn: Optional[_sqlite3.Connection] = None
        # Make sure parent dir exists
        try:
            parent = os.path.dirname(db_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        except Exception as e:
            pass  # silenced - non-essential diagnostic
        # Init schema on a short-lived connection
        self._init_schema()

    def _init_schema(self):
        try:
            conn = _sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
            try:
                conn.execute("PRAGMA journal_mode=WAL;")    # crash-safe, fast writes
                conn.execute("PRAGMA synchronous=NORMAL;")  # safe + fast
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS candles (
                        symbol TEXT NOT NULL,
                        time   INTEGER NOT NULL,
                        open   REAL NOT NULL,
                        high   REAL NOT NULL,
                        low    REAL NOT NULL,
                        close  REAL NOT NULL,
                        volume INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (symbol, time)
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_candles_symbol_time
                    ON candles(symbol, time)
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS gap_state (
                        symbol        TEXT PRIMARY KEY,
                        filled        INTEGER NOT NULL DEFAULT 0,
                        last_fill_at  REAL NOT NULL DEFAULT 0
                    )
                """)
            finally:
                conn.close()
        except Exception as e:
            pass  # silenced - non-essential diagnostic

    # ------------------------------------------------------------------ writes
    def _get_write_conn(self) -> _sqlite3.Connection:
        if self._write_conn is None:
            self._write_conn = _sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
            self._write_conn.execute("PRAGMA journal_mode=WAL;")
            self._write_conn.execute("PRAGMA synchronous=NORMAL;")
        return self._write_conn

    def upsert_candles(self, symbol: str, candles: list) -> int:
        """Insert-or-replace a batch of candles for the given symbol.
        Returns the number of rows written."""
        if not candles:
            return 0
        with self._write_lock:
            conn = self._get_write_conn()
            rows = 0
            try:
                conn.execute("BEGIN IMMEDIATE;")
                for c in candles:
                    conn.execute(
                        "INSERT OR REPLACE INTO candles(symbol, time, open, high, low, close, volume) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?);",
                        (symbol, int(c['time']), float(c['open']), float(c['high']),
                         float(c['low']), float(c['close']), int(c.get('volume', 0)))
                    )
                    rows += 1
                conn.execute("COMMIT;")
            except Exception:
                try: conn.execute("ROLLBACK;")
                except Exception: pass
                pass  # silenced - non-essential diagnostic
                return 0
            return rows

    def append_new_candles(self, symbol: str, candles: list, after_time: int = 0) -> int:
        """Append candles whose time > after_time. Existing candles with time
        <= after_time are left untouched (frozen). Returns number of rows
        actually inserted (not the number replaced)."""
        if not candles:
            return 0
        with self._write_lock:
            conn = self._get_write_conn()
            inserted = 0
            try:
                conn.execute("BEGIN IMMEDIATE;")
                for c in candles:
                    t = int(c['time'])
                    if t <= after_time:
                        continue    # frozen - skip
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO candles(symbol, time, open, high, low, close, volume) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?);",
                        (symbol, t, float(c['open']), float(c['high']),
                         float(c['low']), float(c['close']), int(c.get('volume', 0)))
                    )
                    if cur.rowcount > 0:
                        inserted += 1
                conn.execute("COMMIT;")
            except Exception:
                try: conn.execute("ROLLBACK;")
                except Exception: pass
                pass  # silenced - non-essential diagnostic
                return 0
            return inserted

    def set_gap_filled(self, symbol: str, filled: bool = True, at_ts: float = None):
        if at_ts is None:
            at_ts = time.time()
        with self._write_lock:
            conn = self._get_write_conn()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute(
                    "INSERT INTO gap_state(symbol, filled, last_fill_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(symbol) DO UPDATE SET filled=excluded.filled, last_fill_at=excluded.last_fill_at;",
                    (symbol, 1 if filled else 0, float(at_ts))
                )
                conn.execute("COMMIT;")
            except Exception:
                try: conn.execute("ROLLBACK;")
                except Exception: pass

    def is_gap_filled(self, symbol: str) -> Tuple[bool, float]:
        """Returns (filled, last_fill_at)."""
        try:
            conn = _sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
            try:
                cur = conn.execute(
                    "SELECT filled, last_fill_at FROM gap_state WHERE symbol=?;",
                    (symbol,)
                )
                row = cur.fetchone()
                if row is None:
                    return (False, 0.0)
                return (bool(row[0]), float(row[1]))
            finally:
                conn.close()
        except Exception as e:
            pass  # silenced - non-essential diagnostic
            return (False, 0.0)

    # ------------------------------------------------------------------- reads
    def get_candles(self, symbol: str, limit: Optional[int] = None) -> list:
        """Return all candles for the symbol, sorted by time ascending.
        If limit is given, return only the last `limit` candles."""
        try:
            conn = _sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
            try:
                if limit is None:
                    cur = conn.execute(
                        "SELECT time, open, high, low, close, volume FROM candles "
                        "WHERE symbol=? ORDER BY time ASC;",
                        (symbol,)
                    )
                else:
                    # Use a subselect to get the last N rows by time, then sort ascending.
                    cur = conn.execute(
                        "SELECT time, open, high, low, close, volume FROM candles "
                        "WHERE symbol=? ORDER BY time DESC LIMIT ?;",
                        (symbol, int(limit))
                    )
                    rows = cur.fetchall()
                    rows.reverse()
                    return [{
                        'time': r[0], 'open': r[1], 'high': r[2],
                        'low': r[3], 'close': r[4], 'volume': r[5]
                    } for r in rows]
                rows = cur.fetchall()
                return [{
                    'time': r[0], 'open': r[1], 'high': r[2],
                    'low': r[3], 'close': r[4], 'volume': r[5]
                } for r in rows]
            finally:
                conn.close()
        except Exception as e:
            pass  # silenced - non-essential diagnostic
            return []

    def get_max_time(self, symbol: str) -> int:
        """Return the largest candle timestamp stored for the symbol, or 0."""
        try:
            conn = _sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
            try:
                cur = conn.execute(
                    "SELECT MAX(time) FROM candles WHERE symbol=?;",
                    (symbol,)
                )
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else 0
            finally:
                conn.close()
        except Exception as e:
            pass  # silenced - non-essential diagnostic
            return 0

    def count(self, symbol: str) -> int:
        try:
            conn = _sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
            try:
                cur = conn.execute(
                    "SELECT COUNT(*) FROM candles WHERE symbol=?;",
                    (symbol,)
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0
            finally:
                conn.close()
        except Exception as e:
            pass  # silenced - non-essential diagnostic
            return 0

    def clear_symbol(self, symbol: str) -> int:
        """Delete all candles + gap_state for a symbol (used by full re-fetch)."""
        with self._write_lock:
            conn = self._get_write_conn()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                cur = conn.execute("DELETE FROM candles WHERE symbol=?;", (symbol,))
                deleted = cur.rowcount
                conn.execute("DELETE FROM gap_state WHERE symbol=?;", (symbol,))
                conn.execute("COMMIT;")
                return deleted
            except Exception:
                try: conn.execute("ROLLBACK;")
                except Exception: pass
                return 0

    def clear_gap_state(self, symbol: str) -> None:
        """(تعديل 9) تصفير علامة تجميد السد فقط — الشموع تبقى محفوظة كأرضية
        ثابتة ولا تُمسح إطلاقاً، حتى لا تختفي شموع مُسلَّمة بعد إعادة الجلب."""
        with self._write_lock:
            conn = self._get_write_conn()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute("DELETE FROM gap_state WHERE symbol=?;", (symbol,))
                conn.execute("COMMIT;")
            except Exception:
                try: conn.execute("ROLLBACK;")
                except Exception: pass


# Module-level singleton (created lazily so the path can be picked once
# after APPDATA is available at runtime).
_CANDLE_STORE: Optional[CandleStore] = None
_CANDLE_STORE_LOCK = _threading.Lock()

def get_candle_store() -> CandleStore:
    """Lazy-init the global CandleStore.

    The DB lives NEXT TO THE SCRIPT / EXE (not in APPDATA) so it is easy to
    bundle with a PyInstaller exe and easy for the user to find / back up.
    - When run as a script: candles.db is created in the script's directory.
    - When frozen with PyInstaller (one-file): candles.db is created in the
      directory the exe runs from (sys.executable's parent).
    """
    global _CANDLE_STORE
    if _CANDLE_STORE is not None:
        return _CANDLE_STORE
    with _CANDLE_STORE_LOCK:
        if _CANDLE_STORE is None:
            # Resolve the directory the script / exe lives in.
            # When frozen with PyInstaller, sys.executable points to the exe
            # and we want the DB next to it (not inside the temp _MEIPASS dir).
            if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
                base_dir = os.path.dirname(os.path.abspath(sys.executable))
            else:
                base_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(base_dir, 'candles.db')
            _CANDLE_STORE = CandleStore(db_path)
        return _CANDLE_STORE


# ==============================================================================
# SECTION 9.1: MT4 WRITER (uses CandleStore — SQLite database)
# ==============================================================================
class MT4Writer:
    """ MT4 .
 
 :
 - sync_state[asset] {period, candles, last_write, last_price}
 - seed_history: + HST 
 - update_candle: + HST 200ms
 - fill_gap_once: 20 + HST
 
 :
 - derived_symbol (AUDJPY-OTC) api_symbol 
 - PERIOD=1 (M1) 60 (H1) => AUDJPY-OTC1.hst
 - makedirs + retry + silent errors ( FIX 4/8)
 """
    def __init__(self):
        # (تم نزع MT4) — لم يعد يُنشأ مجلد MT4 ولا يُكتب أي ملف HST؛
        # التخزين الفعلي عبر CandleStore (SQLite) فقط، وسد الثغرات كما هو.
        # ===== FIX 10: sync_state derived_symbol ( AUDJPY-OTC) =====
        # seed_history + update_candle + fill_gap_once
        # => (AUDJPY-OTC1.hst)
        self.sync_state: Dict[str, Any] = {}
        self._writing_queue = set()
        self._gap_filled = {}
        # ===== FIX L8: =====
        # GAP_FILL_REFRESH_INTERVAL ( )
        # => .
        self._gap_filled_at: Dict[str, float] = {}
        # ===== (تعديل 9) أرضية الشموع الثابتة: الشموع المُسلَّمة بعد سد الثغرات
        # تُسجَّل هنا ولا يجوز لأي مسار لاحق حذفها أو استبدالها — الإضافة فقط.
        self._stable_candles: Dict[str, Dict[int, dict]] = {}
        # ---- PERSISTENT STORAGE: SQLite-backed candle store ----------------
        # CandleStore replaces the previous JSON snapshot approach. SQLite is
        # transactional, atomic, indexed by (symbol, time), and far faster than
        # writing/loading a 30 KB JSON file on every cycle. The DB lives at:
        #   %APPDATA%\MetaQuotes\Terminal\Common\Files\qx_hst_snapshots\candles.db
        # See SECTION 9 above for the CandleStore class definition.
        self._candle_store: Optional[CandleStore] = None   # lazy via get_candle_store()

    @property
    def candle_store(self) -> "CandleStore":
        """Lazy accessor so the DB connection is created after APPDATA is set."""
        if self._candle_store is None:
            self._candle_store = get_candle_store()
        return self._candle_store

    def load_snapshot(self, derived_symbol: str) -> Optional[list]:
        """Load candles for an asset from the SQLite store.
        Returns None if no candles exist (so seed_history falls back to the API).
        Also refreshes the in-memory gap_state from the DB."""
        try:
            stored = self.candle_store.get_candles(derived_symbol, limit=INITIAL_CANDLES)
            if not stored:
                return None
            # refresh in-memory gap_state from DB
            filled, last_at = self.candle_store.is_gap_filled(derived_symbol)
            self._gap_filled[derived_symbol] = filled
            self._gap_filled_at[derived_symbol] = last_at
            return stored
        except Exception as e:
            pass  # silenced - non-essential diagnostic
            return None

    def save_snapshot(self, derived_symbol: str, candles: list) -> None:
        """Persist the current candle list to the SQLite store.
        Called after seed_history + after each successful fill_gap_once."""
        try:
            self.candle_store.upsert_candles(derived_symbol, candles)
        except Exception as e:
            pass  # silenced - non-essential diagnostic

    # ------------------------------------------------------------------
    # (تعديل 9) تثبيت الشموع بعد سد الثغرات: إرسال مباشر + بقاء دائم
    # ------------------------------------------------------------------
    def mark_stable(self, derived_symbol: str, candles: list) -> None:
        """يُسجَّل ناتج السد فور اكتماله كأرضية ثابتة: هذه الشموع وصلت فعلاً،
        ومن هذه اللحظة لا يحق لأي مسار لاحق حذفها أو تغيير قيمها."""
        try:
            floor = self._stable_candles.setdefault(derived_symbol, {})
            for c in candles:
                try:
                    t = int(c['time'])
                    floor[t] = {
                        'time': t, 'open': float(c['open']), 'high': float(c['high']),
                        'low': float(c['low']), 'close': float(c['close']),
                        'volume': int(c.get('volume', 0) or 0),
                    }
                except Exception:
                    continue
        except Exception:
            pass

    def stabilize(self, derived_symbol: str, candidate: list) -> list:
        """دمج إضافي فوق الأرضية الثابتة (تعديل 9):
        - كل شمعة مغلقة مُسلَّمة سابقاً تبقى موجودة وبنفس قيمها (لا تختفي، لا تتغير).
        - الشمعة الحية (فترة التيار) يديرها البث المباشر كالمعتاد.
        - الشموع الجديدة تُضاف في النهاية فقط.
        حتى لو أعاد الجلب قائمة ناقصة/مختلفة أو فارغة — الشموع المُسلَّمة تبقى ثابتة."""
        floor = self._stable_candles.get(derived_symbol)
        if not floor:
            return candidate
        try:
            current_start = _align_time(int(time.time()))
            by_time = {}
            for c in candidate:
                try:
                    by_time[int(c['time'])] = c
                except Exception:
                    continue
            for t, c in floor.items():
                if t >= current_start:
                    continue                      # الشمعة الحية — البث يديرها
                by_time[t] = c                    # المغلق المُسلَّم يثبت دائماً
            return [by_time[t] for t in sorted(by_time)]
        except Exception:
            return candidate

    def update_candle(self, asset: str, period: int, tick: dict):
        """ + HST 200ms ( ).

 Args:
 asset: derived_symbol ( "AUDJPY-OTC") — 
 period: MT4 timeframe (PERIOD=1 for M1) — 
 tick: {'time': int, 'price': float}
 """
        if asset not in self.sync_state:
            self.sync_state[asset] = {'period': period, 'candles': [], 'last_write': 0, 'last_price': 0.0}
        state = self.sync_state[asset]
        # ===== FIX L6: _align_time ( tz_offset + ) =====
        # —
        # seed_history fill_gap_once fetch_candles_once.
        aligned_time = _align_time(tick['time'])

        # FIX STALE-CANDLE v2: only the last candle (current/open) can be updated.
        # All candles before it are closed and must never be touched.
        if state['candles']:
            last_time = state['candles'][-1]['time']
            if aligned_time < last_time:
                # tick for a closed minute - ignore (protect closed candles)
                return
            elif aligned_time == last_time:
                last = state['candles'][-1]
                if tick['price'] == state['last_price']: return
                last['high'] = max(last['high'], tick['price'])
                last['low'] = min(last['low'], tick['price'])
                last['close'] = tick['price']
                last['volume'] += 1
            else:
                # new candle - check if there's a gap and fill it with flat candles
                period_secs = PERIOD_SECONDS if PERIOD_SECONDS > 0 else 60
                gap = (aligned_time - last_time) // period_secs
                if gap > 1:
                    # Fill gap with flat candles (use last close as price)
                    last_close = state['candles'][-1]['close']
                    fill_time = last_time + period_secs
                    while fill_time < aligned_time:
                        state['candles'].append({
                            'time': fill_time,
                            'open': last_close,
                            'high': last_close,
                            'low': last_close,
                            'close': last_close,
                            'volume': 0,
                        })
                        fill_time += period_secs
                # Now append the new candle (becomes the last)
                state['candles'].append({
                    'time': aligned_time, 'open': tick['price'], 'high': tick['price'],
                    'low': tick['price'], 'close': tick['price'], 'volume': 1,
                })
        else:
            state['candles'].append({
                'time': aligned_time, 'open': tick['price'], 'high': tick['price'],
                'low': tick['price'], 'close': tick['price'], 'volume': 1,
            })
        if len(state['candles']) > 1000:
            state['candles'] = state['candles'][-1000:]
        state['last_price'] = tick['price']
        now = time.time()
        if now - state['last_write'] > 0.2:
            self.write_to_file(asset, period, state['candles'])
            state['last_write'] = now

    def write_to_file(self, asset: str, period: int, candles: list):
        """(تم نزع MT4) — كانت تكتب {asset}{period}.hst إلى مجلد MT4؛ أصبحت حلقة
        فارغة عمداً حتى يبقى مسار seed_history/fill_gap_once دون أي تعديل."""
        return


    async def seed_history(self, client, asset_obj, period: int, days: float = 0.75):
        """ + HST ( ).
 
 Args:
 client: Quotex
 asset_obj: Asset ( api_symbol symbol)
 period: MT4 timeframe (PERIOD=1 for M1) — get_candles PERIOD_SECONDS
 days: (default 0.75 = 18 )
 
 Returns:
 True False 
 """
        api_symbol = asset_obj.api_symbol
        derived_symbol = asset_obj.symbol

        # ---- SNAPSHOT FAST PATH ------------------------------------------------
        # If we have a previously-saved snapshot for this asset, use it directly.
        # This avoids re-fetching historical candles from the API on every restart
        # and prevents the "candles flicker / disappear and reappear" symptom
        # caused by the API returning slightly different candle data each call.
        # The streaming update_candle() path will simply append new candles as
        # they happen, and fill_gap_once() will only ADD new closed candles
        # (never replace existing ones, since the snapshot marks them as frozen).
        try:
            snap = self.load_snapshot(derived_symbol)
            if snap and len(snap) >= 2:
                snap = sorted(snap, key=lambda x: x['time'])[-INITIAL_CANDLES:]
                # populate sync_state from the snapshot
                self.sync_state[derived_symbol] = {
                    'period': period,
                    'candles': list(snap),
                    'last_write': 0,
                    'last_price': float(snap[-1]['close']) if snap else 0.0,
                }
                # write HST so MT4 picks them up immediately
                self.write_to_file(derived_symbol, period, snap)
                asset_obj.candles = list(snap)
                if snap:
                    asset_obj.price = snap[-1]['close']
                # (تعديل 9) اللقطة المحمَّلة تُسجَّل كأرضية ثابتة مباشرة
                self.mark_stable(derived_symbol, snap)
                # gap is considered already filled (frozen) for the snapshot range
                self._gap_filled[derived_symbol] = True
                self._gap_filled_at[derived_symbol] = time.time()
                pass  # silenced - non-essential diagnostic
                return True
        except Exception as _se:
            pass  # silenced - non-essential diagnostic

        try:
            offset_seconds = int(days * 86400)
            # ===== FIX 10: get_candles PERIOD_SECONDS (60s ) =====
            # PERIOD (1 = M1 timeframe)
            candles = await client.get_candles(api_symbol, time.time(), offset_seconds, PERIOD_SECONDS)
            if candles and len(candles) > 0:
                # ===== FIX L7: _align_time ( tz_offset + ) =====
                # `t = int(c.get('time', ...)) + _tz_offset` —
                # => API 22:36:30 22:36:30
                # update_candle 22:36:00 => => .
                # : _align_time tz_offset + => .
                formatted = []
                for c in candles:
                    t = _align_time(int(c.get('time', c.get('timestamp', 0))))
                    o = float(c.get('open', c.get('o', 0)) or 0)
                    h = float(c.get('high', c.get('max', c.get('h', 0))) or 0)
                    l = float(c.get('low', c.get('min', c.get('l', 0))) or 0)
                    cl = float(c.get('close', c.get('c', 0)) or 0)
                    if o > 0 and h > 0 and l > 0 and cl > 0:
                        formatted.append({
                            'time': t, 'open': o, 'high': h, 'low': l, 'close': cl, 'volume': 0,
                        })
                formatted.sort(key=lambda x: x['time'])
                seen = set()
                unique_candles = [c for c in formatted if not (c['time'] in seen or seen.add(c['time']))]
                # ===== FIX 10: 1000 + =====
                unique_candles = unique_candles[-INITIAL_CANDLES:]
                # ===== FIX 10: sync_state derived_symbol ( AUDJPY-OTC) =====
                self.sync_state[derived_symbol] = {
                    'period': period, 'candles': unique_candles, 'last_write': 0, 'last_price': 0.0,
                }
                # ===== FIX 10: HST derived_symbol + period (AUDJPY-OTC1.hst) =====
                self.write_to_file(derived_symbol, period, unique_candles)
                # asset_obj.candles ( )
                asset_obj.candles = unique_candles
                if unique_candles:
                    asset_obj.price = unique_candles[-1]['close']
                self._gap_filled[derived_symbol] = False
                # ---- Save the freshly-fetched candles as the snapshot so the next
                # restart uses them as-is and never re-fetches this range. ----
                self.save_snapshot(derived_symbol, unique_candles)
                return True
            return False
        except Exception as e:
            # ( spam)
            err_str = str(e).lower()
            if not any(x in err_str for x in ['winerror', 'permission', 'access', 'closed', 'connection']):
                pass  # — main fetch loop
            return False

    async def fill_gap_once(self, client, asset_obj, period: int):
        """ GAP_FILL_CANDLES + .

 (FIX L9):
 1. _align_time tz_offset + => .
 2. GAP_FILL_REFRESH_INTERVAL => .
 3. _fill_missing_slots => .
 4. 0 (volume=0) => .

 (NEW - FREEZE AFTER FIRST SUCCESS):
 5. The original fill_gap_once algorithm is left intact. The ONLY new thing
    is a persistent freeze flag in the SQLite candle store (gap_state.filled).
    Once a successful fill has run for an asset, the flag is set to 1 and
    every subsequent call to fill_gap_once returns immediately without
    touching the candles. This preserves the candles exactly as they were
    after the first fill - no flicker, no replacement of closed candles.
    A 'full re-fetch' command can clear the flag and force a new fill.

 Args:
 client: Quotex
 asset_obj: Asset ( api_symbol symbol)
 period: MT4 timeframe (PERIOD=1 for M1)
 """
        api_symbol = asset_obj.api_symbol
        derived_symbol = asset_obj.symbol

        # 0) FREEZE CHECK: if a successful fill already happened for this asset
        # (gap_state.filled == 1 in the SQLite store), do NOT touch the candles
        # at all. This is the entire fix for the 'candles flicker' symptom -
        # the original algorithm is preserved 1:1, we only skip it after the
        # first successful run.
        try:
            is_filled, _last_at = self.candle_store.is_gap_filled(derived_symbol)
            if is_filled:
                return    # frozen - keep candles exactly as they were
        except Exception:
            pass

        # 1) skip if filled recently (within GAP_FILL_REFRESH_INTERVAL)
        now_ts = time.time()
        last_fill = self._gap_filled_at.get(derived_symbol, 0)
        already_filled = self._gap_filled.get(derived_symbol, False)
        if already_filled and (now_ts - last_fill) < GAP_FILL_REFRESH_INTERVAL:
            return

        # 2) sync_state asset_obj.candles
        if derived_symbol not in self.sync_state:
            if not asset_obj.candles:
                # try to load from store first
                snap = self.load_snapshot(derived_symbol)
                if snap:
                    asset_obj.candles = snap
                else:
                    return
            self.sync_state[derived_symbol] = {
                'period': period,
                'candles': list(asset_obj.candles),
                'last_write': 0,
                'last_price': float(asset_obj.candles[-1]['close']) if asset_obj.candles else 0.0,
            }
        state = self.sync_state[derived_symbol]
        if not state['candles']:
            return

        try:
            # Fetch last GAP_FILL_CANDLES (10) recent candles to replace incomplete ones
            small_days = (GAP_FILL_CANDLES * PERIOD_SECONDS) / 86400.0
            candles = await client.get_candles(api_symbol, time.time(), int(small_days * 86400), PERIOD_SECONDS)
            if not candles:
                # Fill existing gaps even without new data
                merged = sorted(state['candles'], key=lambda x: x['time'])[-INITIAL_CANDLES:]
                merged = _fill_missing_slots(merged, PERIOD_SECONDS, max_gap=1440)
                state['candles'] = merged
                self.write_to_file(derived_symbol, period, merged)
                asset_obj.candles = merged
                self._gap_filled[derived_symbol] = True
                self._gap_filled_at[derived_symbol] = now_ts
                self.save_snapshot(derived_symbol, merged)
                # mark as permanently filled in the DB so the next cycle skips
                self.candle_store.set_gap_filled(derived_symbol, True, now_ts)
                # (تعديل 9) السد اكتمل → الشموع تُسلَّم فوراً وتثبت: لا حذف/استبدال بعدها
                self.mark_stable(derived_symbol, merged)
                return

            # Build candle_map: replace incomplete closed candles with real API data
            # - Current streaming candle (time >= current_period_start): SKIP (streaming handles it)
            # - Closed candles in the fetched range: REPLACE with real data (fixes small bodies)
            # - Missing candles: ADD them
            # - Synthetic candles (volume=0): REPLACE with real data
            current_period_start = _align_time(int(time.time()))
            candle_map: Dict[int, dict] = {}
            for c in state['candles']:
                candle_map[c['time']] = c
            new_count = 0
            replaced_count = 0
            for c in candles:
                t = _align_time(int(c.get('time', c.get('timestamp', 0))))
                o = float(c.get('open', c.get('o', 0)) or 0)
                h = float(c.get('high', c.get('max', c.get('h', 0))) or 0)
                l = float(c.get('low', c.get('min', c.get('l', 0))) or 0)
                cl = float(c.get('close', c.get('c', 0)) or 0)
                v = int(c.get('volume', c.get('vol', 0)) or 0)
                if o > 0 and h > 0 and l > 0 and cl > 0:
                    existing = candle_map.get(t)
                    if existing is None:
                        # genuinely missing candle (real gap) - add it
                        candle_map[t] = {
                            'time': t, 'open': o, 'high': h, 'low': l, 'close': cl, 'volume': v,
                        }
                        new_count += 1
                    elif t >= current_period_start:
                        # current streaming candle - DON'T TOUCH (streaming handles it)
                        continue
                    else:
                        # closed candle in the fetched range - REPLACE with real data
                        # (fixes small/incomplete bodies from partial streaming)
                        candle_map[t] = {
                            'time': t, 'open': o, 'high': h, 'low': l, 'close': cl, 'volume': v,
                        }
                        replaced_count += 1

            # sort + limit to INITIAL_CANDLES
            merged = sorted(candle_map.values(), key=lambda x: x['time'])[-INITIAL_CANDLES:]
            # fill remaining gaps with flat candles (max_gap=1440 = full day)
            merged = _fill_missing_slots(merged, PERIOD_SECONDS, max_gap=1440)
            state['candles'] = merged

            # write HST
            self.write_to_file(derived_symbol, period, merged)

            # update asset_obj.candles (for live price display)
            asset_obj.candles = merged
            if merged:
                asset_obj.price = merged[-1]['close']

            # 9) mark gap as filled
            self._gap_filled[derived_symbol] = True
            self._gap_filled_at[derived_symbol] = now_ts
            # 10) persist candles to the SQLite store (so the next restart
            # loads them from the DB instead of re-fetching from the API).
            self.save_snapshot(derived_symbol, merged)
            # 11) set the permanent freeze flag - subsequent fill_gap_once
            # calls for this asset will return immediately without touching
            # the candles. This is the entire fix for the 'flicker' symptom.
            self.candle_store.set_gap_filled(derived_symbol, True, now_ts)
            # (تعديل 9) السد اكتمل → الشموع تُسلَّم فوراً وتثبت: لا حذف/استبدال بعدها
            self.mark_stable(derived_symbol, merged)
        except Exception as e:
            err_str = str(e).lower()
            if not any(x in err_str for x in ['winerror', 'permission', 'access', 'closed', 'connection']):
                pass

MT4_WRITER = MT4Writer()

async def mt4_gap_filler():
    global GAP_FILL_SUMMARY, GAP_FILL_ACTIVE
    # Skip entirely for subsecond mode — no gap filler for 5s/10s/15s/30s
    if IS_SUBSECOND:
        GAP_FILL_SUMMARY = "GAP FILL: disabled (subsecond mode)"
        GAP_FILL_ACTIVE = False
        return
    total = len(ALL_STREAMING_ASSETS)
    GAP_FILL_SUMMARY = f"GAP FILL: waiting 60s before starting ({total} assets)..."
    await asyncio.sleep(60)
    cycle = 0
    while True:
        cycle += 1
        if not ALL_STREAMING_ASSETS:
            GAP_FILL_SUMMARY = "GAP FILL: no assets to fill"
            GAP_FILL_ACTIVE = False
            await asyncio.sleep(GAP_FILL_REFRESH_INTERVAL)
            continue
        GAP_FILL_ACTIVE = True  # FIX L10:
        filled_count = 0
        skipped_count = 0
        failed_count = 0
        failed_assets = []
        t0 = time.time()
        for i, asset in enumerate(ALL_STREAMING_ASSETS, 1):
            if not CLIENT or not CLIENT.api:
                GAP_FILL_SUMMARY = f"GAP FILL: ABORTED (connection lost) | {filled_count}/{total} filled | {skipped_count} skipped"
                GAP_FILL_ACTIVE = False

                await asyncio.sleep(5)
                break
            try:
                await MT4_WRITER.fill_gap_once(CLIENT, asset, PERIOD)
                if MT4_WRITER._gap_filled.get(asset.symbol, False):
                    filled_count += 1
                else:
                    skipped_count += 1
                await asyncio.sleep(0.3)
            except Exception:
                skipped_count += 1
                failed_count += 1
                failed_assets.append(asset.symbol)
            # —
            GAP_FILL_SUMMARY = (
                f"GAP FILL c{cycle}: {i}/{total} done | {filled_count} filled | "
                f"{skipped_count} skipped | {failed_count} failed | {time.time()-t0:.1f}s"
            )
        else:
            # ( connection loss)
            elapsed = time.time() - t0
            fail_str = f" | Failed: {', '.join(failed_assets[:5])}" if failed_assets else ""
            GAP_FILL_SUMMARY = (
                f"GAP FILL c{cycle}: COMPLETE in {elapsed:.1f}s | {filled_count}/{total} filled | "
                f"{skipped_count} skipped{fail_str} | next in {GAP_FILL_REFRESH_INTERVAL}s"
            )
            GAP_FILL_ACTIVE = False
            # (5 )
            await asyncio.sleep(GAP_FILL_REFRESH_INTERVAL)
            # ( / )
            total = len(ALL_STREAMING_ASSETS)
            continue
        # connection loss —
        GAP_FILL_ACTIVE = False
        await asyncio.sleep(GAP_FILL_REFRESH_INTERVAL)
        total = len(ALL_STREAMING_ASSETS)

def print_dashboard():
    _tf_label = "Unknown"
    for tf_tuple in TIMEFRAMES:
        mt4_p, secs, label = tf_tuple[0], tf_tuple[1], tf_tuple[2]
        if mt4_p == PERIOD and secs == PERIOD_SECONDS:
            _tf_label = label
            break
    # تبسيط العنوان: سطر واحد فاصل بدل 3 خطوط زرقاء كثيفة
    print(f"  {Colors.DIM}{'─'*68}{Colors.RESET}")
    print(f"  {Colors.BOLD}QX Console - v14  {Colors.DIM}|  Default: {DEFAULT_ASSET}{Colors.RESET}")
    print(f"  {Colors.DIM}{'─'*68}{Colors.RESET}")
    print(f"  {Colors.CYAN}Timeframe  {Colors.RESET}: {_tf_label} ({PERIOD_SECONDS}s)")
    print(f"  {Colors.CYAN}History    {Colors.RESET}: {HISTORY_DAYS:.2f} day = {INITIAL_CANDLES} candles")
    # FIX L11: TZ offset display
    _tz_sec = _get_tz_offset()
    _tz_sign = '+' if _tz_sec >= 0 else '-'
    _tz_h, _tz_m = divmod(abs(_tz_sec) // 60, 60)
    _tz_src = "manual" if LOCAL_TZ_OFFSET_SECONDS is not None else "system"
    print(f"  {Colors.CYAN}TZ offset  {Colors.RESET}: UTC{_tz_sign}{_tz_h:02d}:{_tz_m:02d} (source={_tz_src})")
    print(f"  {Colors.CYAN}Gap fill   {Colors.RESET}: {GAP_FILL_CANDLES} candles / {GAP_FILL_REFRESH_INTERVAL}s")
    print(f"  {Colors.GREEN}Contact    {Colors.RESET}: https://t.me/qxchart")
    print(f"  {Colors.DIM}{'─'*68}{Colors.RESET}\n")

if __name__ == "__main__":
    # ════════════════════════════════════════════════════════════════════
    # STAGE 1: STARTUP — ترحيب مباشر (نظام الترخيص أُزيل بالكامل بناءً على الطلب)
    qx_print_welcome()
    # ════════════════════════════════════════════════════════════════════

    # ════════════════════════════════════════════════════════════════════
    # STAGE 2: Select Timeframe — 3 أعمدة × 5 صفوف (بدون خطوط زرقاء كثيرة)
    # ════════════════════════════════════════════════════════════════════
    # ترتيب الفريمات حسب الطلب:
    # العمود الأول (1-5): M1, M2, M3, M5, M10
    # العمود الثاني (6-10): M15, M30, H1, H4, D1
    # العمود الثالث (11-14): S5, S10, S15, S30
    _TF_DISPLAY_ORDER = [
        # (idx, mt4_period, seconds, short_label, full_label)
        (1,  1,    60,    "M1",  "1 minute"),
        (2,  2,    120,   "M2",  "2 minutes"),
        (3,  3,    180,   "M3",  "3 minutes"),
        (4,  5,    300,   "M5",  "5 minutes"),
        (5,  10,   600,   "M10", "10 minutes"),
        (6,  15,   900,   "M15", "15 minutes"),
        (7,  30,   1800,  "M30", "30 minutes"),
        (8,  60,   3600,  "H1",  "1 hour"),
        (9,  240,  14400, "H4",  "4 hours"),
        (10, 1440, 86400, "D1",  "1 day"),
        (11, 1,    5,     "S5",  "5 seconds"),
        (12, 1,    10,    "S10", "10 seconds"),
        (13, 1,    15,    "S15", "15 seconds"),
        (14, 1,    30,    "S30", "30 seconds"),
    ]
    # خريطة (idx → is_subsecond)
    _TF_SUBSEC = {11: True, 12: True, 13: True, 14: True}

    print()
    print(f"{Colors.BOLD}  Select Timeframe{Colors.RESET}")
    print(f"  {Colors.DIM}{'═' * 55}{Colors.RESET}")
    # عرض القائمة في 3 أعمدة × 5 صفوف
    # صف 0: idx 1,6,11 | صف 1: 2,7,12 | صف 2: 3,8,13 | صف 3: 4,9,14 | صف 4: 5,10,—
    _ROWS = [
        (1, 6, 11),   # M1   M15   S5
        (2, 7, 12),   # M2   M30   S10
        (3, 8, 13),   # M3   H1    S15
        (4, 9, 14),   # M5   H4    S30
        (5, 10, None),# M10  D1    —
    ]
    _tf_lookup = {row[0]: row for row in _TF_DISPLAY_ORDER}
    # ثوابت المحاذاة: كل عمود 17 حرف ظاهر، فجوة 2 حرف بين الأعمدة
    # → كل صف = 2 (indent) + 17*3 + 2*2 = 57 حرف (مطابق لخط ═ 55 + 2 indent)
    # الصيغة: "XX - XXXXX - XXX" = 2 + 3 + 5 + 3 + 4 = 17 حرف ظاهر لكل خلية
    _TF_COL_WIDTH = 17
    _TF_GAP = "  "  # فجوة صغيرة بين الأعمدة (قرّب الأعمدة من بعضها)
    for row in _ROWS:
        cells = []
        for idx in row:
            if idx is None:
                # ملء بمسافات بنفس عرض العمود (يحافظ على تساوي طول الصف)
                cells.append(" " * _TF_COL_WIDTH)
                continue
            entry = _tf_lookup[idx]
            sec = entry[2]      # seconds (int)
            label = entry[3]    # short_label (M1, S30, ...)
            # التنسيق: idx(يمين 2) + " - " + sec(يسار 5) + " - " + label(يسار 4) = 17 حرف
            # Cyan = لون الشعار (بدل Yellow)
            cells.append(f"{Colors.CYAN}{idx:>2}{Colors.RESET} - {sec:<5} - {label:<4}")
        print(f"  {cells[0]}{_TF_GAP}{cells[1]}{_TF_GAP}{cells[2]}")
    print(f"  {Colors.DIM}{'═' * 55}{Colors.RESET}")
    while True:
        try:
            tf_choice = input(f"{Colors.CYAN}  Choose 1-14 (default=1 for M1): {Colors.RESET}").strip()
            if not tf_choice:
                _chosen_idx = 1
            else:
                _chosen_idx = int(tf_choice)
                if not (1 <= _chosen_idx <= 14):
                    print(f"{Colors.RED}  Invalid choice (1-14).{Colors.RESET}")
                    continue
            break
        except (ValueError, KeyboardInterrupt):
            print(f"{Colors.RED}  Invalid input.{Colors.RESET}")
        except EOFError:
            _chosen_idx = 1
            break
    _tf_entry = _tf_lookup[_chosen_idx]
    PERIOD, PERIOD_SECONDS = _tf_entry[1], _tf_entry[2]
    _tf_short = _tf_entry[2]
    _tf_full = _tf_entry[3]
    _is_subsecond = _TF_SUBSEC.get(_chosen_idx, False)
    globals()['PERIOD'] = PERIOD
    globals()['PERIOD_SECONDS'] = PERIOD_SECONDS
    if PERIOD_SECONDS < 60:
        globals()['IS_SUBSECOND'] = True
        globals()['SUBSECOND_SECONDS'] = PERIOD_SECONDS
        _subsec_label = f"{PERIOD_SECONDS}S"
        globals()['SUBSECOND_LABEL'] = _subsec_label
        print(f"{Colors.GREEN}  Using timeframe: {_tf_short} - {_tf_full}{Colors.RESET}")
        print(f"{Colors.DIM}  Sub-minute: fetches period=1 history + aggregates locally{Colors.RESET}\n")
    else:
        globals()['IS_SUBSECOND'] = False
        globals()['SUBSECOND_SECONDS'] = 0
        globals()['SUBSECOND_LABEL'] = ""
        print(f"{Colors.GREEN}  Using timeframe: {_tf_short} - {_tf_full}{Colors.RESET}\n")

    # ===== v15: اختيار طريقة جلب التاريخ =====
    print()
    print(f"{Colors.BOLD}  History method{Colors.RESET}")
    print(f"  {Colors.CYAN} 1{Colors.RESET} - Browser-style (history/load backwards, like the web chart)")
    print(f"  {Colors.CYAN} 2{Colors.RESET} - Legacy (original method)")
    try:
        _hm_choice = input(f"{Colors.CYAN}  Choose 1-2 (default=1): {Colors.RESET}").strip()
    except EOFError:
        _hm_choice = ""
    globals()['HISTORY_METHOD'] = "legacy" if _hm_choice == "2" else "browser"
    print(f"{Colors.GREEN}  History method: {HISTORY_METHOD}{Colors.RESET}\n")

    # ════════════════════════════════════════════════════════════════════
    # STAGE 3: Select History Size
    # للفريمات الفرعية (subsecond) نستخدم قيمة ثابتة ولا نسأل المستخدم
    # لأن النظام يجلب period=1 ويُجمّع محلياً — عدد الأيام غير ذي معنى هنا
    # ════════════════════════════════════════════════════════════════════
    if PERIOD_SECONDS < 60:
        # الفريمات الفرعية (5S/10S/15S/30S): قيمة ثابتة بدون سؤال
        # ===== v15: ساعة كاملة للفريمات الفرعية عند أسلوب المتصفح =====
        if _use_browser_history():
            SUBSECOND_HISTORY_CANDLES = SUBSECOND_HISTORY_SECONDS // PERIOD_SECONDS
            globals()['SUBSECOND_HISTORY_CANDLES'] = SUBSECOND_HISTORY_CANDLES
        INITIAL_CANDLES = SUBSECOND_HISTORY_CANDLES
        HISTORY_DAYS = (INITIAL_CANDLES * PERIOD_SECONDS) / 86400.0 * 1.15
        FETCH_DURATION_SECONDS = int(86400 * HISTORY_DAYS)
        globals()['INITIAL_CANDLES'] = INITIAL_CANDLES
        globals()['HISTORY_DAYS'] = HISTORY_DAYS
        globals()['FETCH_DURATION_SECONDS'] = FETCH_DURATION_SECONDS
        # رسالة قصيرة بدل قائمة كاملة
        print(f"{Colors.DIM}  Sub-second timeframe: auto history ({INITIAL_CANDLES} candles, {HISTORY_DAYS:.2f} days){Colors.RESET}\n")
    else:
        # (طلب المستخدم) جلب ثابت بدون سؤال: 0.80 يوم من الشموع
        HISTORY_DAYS = 0.80
        INITIAL_CANDLES = max(int((HISTORY_DAYS * 86400) / PERIOD_SECONDS), 100)
        FETCH_DURATION_SECONDS = int(86400 * HISTORY_DAYS)
        globals()['INITIAL_CANDLES'] = INITIAL_CANDLES
        globals()['HISTORY_DAYS'] = HISTORY_DAYS
        globals()['FETCH_DURATION_SECONDS'] = FETCH_DURATION_SECONDS
        print(f"{Colors.GREEN}  History: fixed 0.80 day = {INITIAL_CANDLES} candles{Colors.RESET}\n")

    # (تم حذف كتلة Dashboard بناءً على طلب المستخدم — الترحيب + القوائم كافية)

    # ════════════════════════════════════════════════════════════════════
    # STAGE 4: Domain — التعرف التلقائي على البلد عبر IP (بدون قائمة يدوية)
    #   بنغلاديش (BD)   -> market-qx.info   (نطاق بنغلاديش)
    #   أي دولة أخرى    -> quotex.com       (النطاق الرسمي العالمي)
    # يتم التعرف على "البلد فقط" وليس الموقع الدقيق.
    # ════════════════════════════════════════════════════════════════════
    print()
    print(f"{Colors.BOLD}  Domain: detecting country automatically ...{Colors.RESET}")
    resolve_qx_host()
    globals()['QX_HOST'] = QX_HOST
    _geo_country = _GEO_CACHE.get("country") or "unknown"
    print(f"{Colors.GREEN}  Country: {_geo_country}  ->  Domain: {QX_HOST}{Colors.RESET}\n")


    # ════════════════════════════════════════════════════════════════════
    # STAGE 5: تسجيل دخول Quotex
    # ════════════════════════════════════════════════════════════════════
    # Wipe the sessions table on every launch so the app always starts from a
    # clean state — no stale cookies/token from a previous run. The saved
    # credentials (email + password) live in app_state, which is intentionally
    # preserved so the user does not have to retype them.
    session_manager.delete_session_file()
    session_manager.delete_browser_dir()
    should_force = session_manager.should_force_fresh()
    saved_email = session_manager.get_saved_email()
    saved_password = session_manager.get_saved_password()
    success = False
    # Case 1: both email and password are saved AND the previous login was
    # healthy (not too old, not too many failed attempts) → auto-login with
    # force_fresh=True (the sessions table is already empty).
    if saved_email and saved_password and not should_force:
        print(f"{Colors.GREEN}Found saved credentials: {saved_email}{Colors.RESET}")
        fut = asyncio.run_coroutine_threadsafe(connect_quotex(saved_email, saved_password, force_fresh=True, max_attempts=3), ASYNC_LOOP)
        success = fut.result(timeout=180)
    # Case 2: saved email only (no password on file) → ask for the password.
    if not success and saved_email and not should_force:
        print(f"{Colors.GREEN}Found saved email: {saved_email}{Colors.RESET}")
        PASSWORD_INPUT = input(f"{Colors.CYAN} Password: {Colors.RESET}").strip()
        fut = asyncio.run_coroutine_threadsafe(connect_quotex(saved_email, PASSWORD_INPUT, force_fresh=True, max_attempts=3), ASYNC_LOOP)
        success = fut.result(timeout=180)
    # Case 3: no usable saved credentials → prompt for both email and password.
    if not success:
        print()
        EMAIL_INPUT = input(f"{Colors.CYAN}Email: {Colors.RESET}").strip()
        PASSWORD_INPUT = input(f"{Colors.CYAN}Password: {Colors.RESET}").strip()
        fut = asyncio.run_coroutine_threadsafe(connect_quotex(EMAIL_INPUT, PASSWORD_INPUT, force_fresh=True, max_attempts=3), ASYNC_LOOP)
        success = fut.result(timeout=300)
    if not success:
        print(f"\n{Colors.RED}[FAILED] Could not connect after all attempts.{Colors.RESET}")
        sys.exit(1)
        

    total_assets = len(ASSET_LIST)
    # ===== FIX 8: 1 — =====
    print(f"\n{Colors.CYAN}{'═'*60}{Colors.RESET}")
    print(f"{Colors.BOLD}  SECTION 1: CANDLE FETCHING ({total_assets} assets, {INITIAL_CANDLES} candles each){Colors.RESET}")
    print(f"{Colors.CYAN}{'═'*60}{Colors.RESET}\n")

    all_assets = []
    total_loaded = 0
    consecutive_disconnects = 0
    for idx, symbol in enumerate(ASSET_LIST, 1):
        # ===== FIX 3: =====
        # auto_reconnect .
        # ASYNC_LOOP run_coroutine_threadsafe
        # main async.
        if not CONNECTION_ALIVE or CLIENT is None or CLIENT.api is None:
            logmsg(f"[fetch_loop] connection dead before asset {idx}/{total_assets}; waiting for reconnect...")
            async def _wait_reconnect(timeout=30.0):
                try:
                    await wait_until(
                        lambda: CONNECTION_ALIVE and CLIENT is not None and CLIENT.api is not None,
                        timeout=timeout,
                        poll_interval=0.5,
                    )
                    return True
                except asyncio.TimeoutError:
                    return False
            reconnected = asyncio.run_coroutine_threadsafe(_wait_reconnect(30.0), ASYNC_LOOP).result(timeout=35)
            if reconnected:
                consecutive_disconnects = 0
            else:
                consecutive_disconnects += 1
                logmsg(f"[fetch_loop] reconnect failed within 30s (streak={consecutive_disconnects})")
                if consecutive_disconnects >= MAX_RECONNECTS_DURING_FETCH:
                    logmsg(f"[fetch_loop] giving up after {consecutive_disconnects} failed reconnects")
                    break
                continue
        asset = Asset(symbol)
        display = pretty_asset_name(symbol)
        all_assets.append(asset)
        ALL_STREAMING_ASSETS.append(asset)
        start_time = time.time()
        # Use 1.0s delay between assets for sub-minute (server sensitive to period=1)
        # 0.1s for 60s+ timeframes (normal speed)
        _asset_delay = 1.0 if PERIOD_SECONDS < 60 else FETCH_ASSET_DELAY
        # Start realtime_stream FIRST (before historical fetch)
        # This way the stream collects live ticks DURING the fetch, closing the gap
        # between historical last candle and live first candle
        asset.stream_task = asyncio.run_coroutine_threadsafe(realtime_stream(asset), ASYNC_LOOP)
        # Small delay to let stream subscribe before fetch
        time.sleep(0.1)
        # Now fetch historical candles (stream is collecting live ticks in parallel)
        fut = asyncio.run_coroutine_threadsafe(fetch_candles_with_retry(asset, max_retries=MAX_FETCH_RETRIES, idx=idx, total=total_assets), ASYNC_LOOP)
        try:
            candles_count, attempts = fut.result(timeout=180)
            elapsed = time.time() - start_time
            if candles_count >= MIN_CANDLES_THRESHOLD:
                total_loaded += candles_count
                consecutive_disconnects = 0
                attempts_str = "" if attempts == 1 else f" ({attempts} attempts)"
                print(f"\r{Colors.CLEAR_LINE}  {Colors.CYAN}{display:<25}{Colors.RESET} {Colors.GREEN}{candles_count} candles in {elapsed:.1f}s{attempts_str}{Colors.RESET} Streaming")
            else:
                print(f"\r{Colors.CLEAR_LINE}  {Colors.CYAN}{display:<25}{Colors.RESET} {Colors.RED}Only {candles_count} candles{Colors.RESET} Streaming")
            # ===== v16: حفظ JSON مباشرة بعد اكتمال جلب شموع هذه العملة =====
            if candles_count > 0:
                _saved = _save_candles_json(asset, source_label="startup_fetch")
                if _saved:
                    print(f"  {Colors.DIM}→ Saved: {_saved.name} ({candles_count} candles){Colors.RESET}")
        except Exception as e:
            print(f"\r{Colors.CLEAR_LINE}  {Colors.CYAN}{display:<25}{Colors.RESET} {Colors.RED}Error: {str(e)[:30]}{Colors.RESET}")
            if "Connection" in str(e) or "closed" in str(e).lower():
                consecutive_disconnects += 1
                if consecutive_disconnects >= MAX_RECONNECTS_DURING_FETCH:
                    logmsg(f"[fetch_loop] too many disconnects ({consecutive_disconnects}); aborting fetch")
                    break
        # Merge: any live ticks collected during fetch are already in asset.candles
        # fetch_candles_with_retry overwrites asset.candles with historical only.
        # To preserve live ticks, merge them back after fetch.
        # (handled inside realtime_stream which appends to asset.candles)
        # ===== FIX 7: cooldown — =====
        # . keepalive_loop .
        if FETCH_COOLDOWN_EVERY > 0 and idx > 0 and idx % FETCH_COOLDOWN_EVERY == 0 and idx < total_assets:
            logmsg(f"[fetch_loop] cooldown {FETCH_COOLDOWN_DURATION}s after {idx}/{total_assets} assets")
            time.sleep(FETCH_COOLDOWN_DURATION)
        else:
            # Use 1.0s for sub-minute (server sensitive to period=1), 0.1s for 60s+
            time.sleep(_asset_delay)

    # NOTE: gap filler runs automatically on its interval (every GAP_FILL_REFRESH_INTERVAL).
    # First cycle waits 60s, then runs periodically. Not forced at startup.
    # Gap fill respects the user's chosen timeframe via dynamic _align_time/_fill_missing_slots.

    asyncio.run_coroutine_threadsafe(health_monitor(), ASYNC_LOOP)
    asyncio.run_coroutine_threadsafe(auto_reconnect(), ASYNC_LOOP)
    asyncio.run_coroutine_threadsafe(keepalive_loop(), ASYNC_LOOP)
    asyncio.run_coroutine_threadsafe(stale_message_watchdog(), ASYNC_LOOP)
    # start the automatic gap filler loop (runs every GAP_FILL_REFRESH_INTERVAL)
    asyncio.run_coroutine_threadsafe(mt4_gap_filler(), ASYNC_LOOP)
    # ===== قائمة بقية العملات (نسبة الدفع + مفتوح/مغلق) — v16 FIX =====
    # السبب الذي كانت تختفي بسببه: كانت تُجدول خلفياً فقط، فيطبع السكربت صندوق
    # الملخص ويبدأ جدول الحالة المباشر فوراً (يعيد الرسم كل 0.15s) ويطمس القائمة
    # المطبوعة. الآن نطبعها **حاجبةً هنا** — قبل صندوق الملخص وجدول الحالة — لتبقى ظاهرة.
    _assets_fut = asyncio.run_coroutine_threadsafe(send_assets_list_once(), ASYNC_LOOP)
    try:
        _assets_fut.result(timeout=45)
    except Exception as _e:
        logmsg(f"[assets] asset list failed: {_e}")
    
    # ===== FIX 8: dashboard — 3 =====
    # 1: ( )
    # 2: ( status line )
    # 3: ( mt4_gap_filler)
    print(f"\n{Colors.GREEN}{'═'*60}{Colors.RESET}")
    print(f"{Colors.BOLD}  All assets loaded and streaming!{Colors.RESET}")
    print(f"  Total: {total_loaded} candles for {len(all_assets)} assets")
    print(f"  History window: {HISTORY_DAYS} day (optimized, fast fetch)")
    print(f"  Storage: SQLite database (candles.db)")
    print(f"  Stream Poll: {STREAM_POLL_INTERVAL}s")
    print(f"  Health Monitor: ACTIVE (Lenient: ignores websocket flickers)")
    print(f"  Keepalive Ping: ACTIVE (every {KEEPALIVE_PING_INTERVAL}s, multi-type)")
    print(f"  Stale Watchdog: ACTIVE (recycles if silent > {STALE_MESSAGE_TIMEOUT}s)")
    print(f"  Auto-Reconnect: instant detect + exponential backoff (never gives up)")
    print(f"  Internal Watchdog: ACTIVE (recycles stale connections > 90s)")
    print(f"  Fetch: {FETCH_MAX_WORKERS} workers, {FETCH_CHUNK_SIZE}c/batch, {FETCH_BATCH_DELAY}s delay, {FETCH_ASSET_DELAY}s between assets")
    print(f"  Retry: {MAX_FETCH_RETRIES} attempts, backoff {RETRY_BACKOFF_BASE}-{RETRY_BACKOFF_MAX}s")
    print(f"{Colors.GREEN}{'═'*60}{Colors.RESET}\n")

    # ===== FIX E/F/G: — =====
    # FIX E: ( 6)
    # FIX F: ( print() )
    # FIX G: ( / ) +
    # FIX H: (columns/rows) —

    import shutil as _shutil
    def _get_term_size():
        try:
            s = _shutil.get_terminal_size((80, 24))
            return s.columns, s.lines
        except Exception:
            return 80, 24

    def _fmt_price(p):
        if p <= 0: return "—"
        if p >= 1000: return f"{p:.3f}"
        if p >= 100:  return f"{p:.3f}"
        if p >= 1:    return f"{p:.5f}"
        return f"{p:.5f}"

    def _draw_live_table(first_draw=False):
        cols, rows = _get_term_size()
        # ===== FIX H: — =====
        # : 25 chars (name=12 + price=10 + )
        # : 50 chars (name=25 + price=15 + status=8 + )
        cols = max(25, cols)
        if cols >= 55:
            name_w, price_w = 25, 15
            show_status = True
        elif cols >= 40:
            name_w, price_w = 20, 12
            show_status = True
        elif cols >= 30:
            name_w, price_w = 15, 10
            show_status = False
        else:
            name_w, price_w = 12, 8
            show_status = False
        bar_w = max(20, min(70, cols - 2))
        # ( )
        header_lines = 3
        data_lines = len(all_assets) + 1  # +1
        gap_lines = 0
        if GAP_FILL_SUMMARY:
            gap_lines = 3  # ===, summary, ===
        # status messages lines (queued log messages)
        msg_lines_count = min(len(_STATUS_MESSAGES_QUEUE), 3)
        msg_lines = msg_lines_count + (1 if msg_lines_count > 0 else 0)  # +1 for separator
        total_lines = header_lines + data_lines + gap_lines + msg_lines
        if not first_draw:
            sys.stdout.write(f"\033[{total_lines}A")
        # ===== =====
        sys.stdout.write(f"\033[2K{Colors.CYAN}{'═'*bar_w}{Colors.RESET}\n")
        sys.stdout.write(f"\033[2K{Colors.BOLD}  LIVE PRICES{Colors.RESET}\n")
        sys.stdout.write(f"\033[2K{Colors.CYAN}{'═'*bar_w}{Colors.RESET}\n")
        # ===== =====
        for asset in all_assets:
            display = pretty_asset_name(asset.api_symbol)
            # name_w
            if len(display) > name_w:
                display = display[:name_w-1] + "~"
            # FIX G:
            name_visible = f"{display:<{name_w}}"
            name_colored = f"{Colors.BLUE}{name_visible}{Colors.RESET}"
            # FIX G: ( / )
            price_str = _fmt_price(asset.price)
            if asset.previous_price > 0 and asset.price > 0:
                if asset.price > asset.previous_price:
                    price_colored = f"{Colors.GREEN}{price_str:<{price_w}}{Colors.RESET}"
                elif asset.price < asset.previous_price:
                    price_colored = f"{Colors.RED}{price_str:<{price_w}}{Colors.RESET}"
                else:
                    price_colored = f"{price_str:<{price_w}}"
            else:
                price_colored = f"{price_str:<{price_w}}"

            if show_status:
                if asset.updates > 0:
                    status_str = f"{Colors.GREEN}LIVE{Colors.RESET}"
                else:
                    status_str = f"{Colors.RED}WAIT{Colors.RESET}"
                sys.stdout.write(f"\033[2K  {name_colored} {price_colored} {status_str}\n")
            else:
                sys.stdout.write(f"\033[2K  {name_colored} {price_colored}\n")

        if CONNECTION_ALIVE:
            conn_status = f"{Colors.GREEN}CONNECTED{Colors.RESET}"
        else:
            conn_status = f"{Colors.RED}RECONNECTING...{Colors.RESET}"
        if show_status:
            sys.stdout.write(f"\033[2K  {Colors.BLUE}{'Connection':<{name_w}}{Colors.RESET} {'':<{price_w}} {conn_status}\n")
        else:
            sys.stdout.write(f"\033[2K  {Colors.BLUE}{'Connection':<{name_w}}{Colors.RESET} {conn_status}\n")
        # ===== FIX F: ( ) =====
        if GAP_FILL_SUMMARY:
            summary = GAP_FILL_SUMMARY
            # bar_w-4
            max_len = bar_w - 4
            if len(summary) > max_len:
                summary = summary[:max_len-3] + "..."
            # :
            if "ABORTED" in GAP_FILL_SUMMARY or "Failed:" in GAP_FILL_SUMMARY and "0 failed" not in GAP_FILL_SUMMARY:
                summary_colored = f"{Colors.RED}{summary}{Colors.RESET}"
            elif "COMPLETE" in GAP_FILL_SUMMARY:
                summary_colored = f"{Colors.GREEN}{summary}{Colors.RESET}"
            elif GAP_FILL_ACTIVE:
                summary_colored = f"{Colors.CYAN}{summary}{Colors.RESET}"
            else:
                summary_colored = summary
            sys.stdout.write(f"\033[2K{Colors.CYAN}{'═'*bar_w}{Colors.RESET}\n")
            sys.stdout.write(f"\033[2K  {summary_colored}\n")
            sys.stdout.write(f"\033[2K{Colors.CYAN}{'═'*bar_w}{Colors.RESET}\n")
        # Display queued log messages (last 3)
        if _STATUS_MESSAGES_QUEUE:
            sys.stdout.write(f"\033[2K{Colors.DIM}{'─'*bar_w}{Colors.RESET}\n")
            for msg in _STATUS_MESSAGES_QUEUE[-3:]:
                # truncate to bar_w-4
                if len(msg) > bar_w - 4:
                    msg = msg[:bar_w-7] + "..."
                sys.stdout.write(f"\033[2K  {Colors.DIM}{msg}{Colors.RESET}\n")
        sys.stdout.flush()

    # Initial draw
    _draw_live_table(first_draw=True)
    print()
    print(f"{Colors.CYAN}Live streaming active. Press Enter for menu, Ctrl+C to stop.{Colors.RESET}")
    print()

    # Interactive menu state
    stop_requested = False
    reinit_requested = False
    _enter_pressed = threading.Event()

    def _stdin_listener():
        while not stop_requested and not reinit_requested:
            try:
                line = sys.stdin.readline()
                if line == '' or line is None:
                    time.sleep(0.5)
                    continue
                _enter_pressed.set()
                while _enter_pressed.is_set() and not stop_requested and not reinit_requested:
                    time.sleep(0.1)
            except (KeyboardInterrupt, EOFError):
                time.sleep(0.5)
            except Exception:
                time.sleep(0.5)

    _stdin_thread = threading.Thread(target=_stdin_listener, daemon=True, name="StdinListener")
    _stdin_thread.start()

    def _show_interactive_menu():
        print()
        print(f"{Colors.CYAN}{'═'*70}{Colors.RESET}")
        print(f"{Colors.BOLD}  MENU{Colors.RESET}")
        print(f"{Colors.CYAN}{'═'*70}{Colors.RESET}")
        print(f"  {Colors.CYAN}1{Colors.RESET} - Change Timeframe (current: {_current_period_label()})")
        print(f"  {Colors.CYAN}2{Colors.RESET} - Change History days (current: {HISTORY_DAYS:.2f}d = {INITIAL_CANDLES} candles)")
        print(f"  {Colors.CYAN}3{Colors.RESET} - Change both timeframe and history days")
        print(f"  {Colors.CYAN}4{Colors.RESET} - Resume streaming (continue)")
        print(f"  {Colors.CYAN}5{Colors.RESET} - Exit program")
        print(f"{Colors.CYAN}{'═'*70}{Colors.RESET}")
        while True:
            try:
                c = input(f"{Colors.CYAN}Choose 1-5: {Colors.RESET}").strip()
                if c in ('1', '2', '3', '4', '5'):
                    return int(c)
                print(f"{Colors.RED}Invalid choice.{Colors.RESET}")
            except (ValueError, KeyboardInterrupt):
                print(f"{Colors.RED}Invalid input.{Colors.RESET}")
            except EOFError:
                return 4

    def _choose_timeframe():
        # نفس تنسيق القائمة الأولية: 3 أعمدة × 5 صفوف
        # The cell layout must match Stage 2 exactly: "XX - SSSSS - LLLL" (17 chars).
        # The current item is highlighted in GREEN instead of an inline asterisk,
        # so column widths stay perfectly aligned with the initial menu.
        print()
        print(f"{Colors.BOLD}  Select Timeframe{Colors.RESET}")
        print(f"  {Colors.DIM}{'═' * 55}{Colors.RESET}")
        _ROWS_LOCAL = [
            (1, 6, 11), (2, 7, 12), (3, 8, 13), (4, 9, 14), (5, 10, None),
        ]
        _TF_COL_WIDTH = 17
        _TF_GAP = "  "
        for row in _ROWS_LOCAL:
            cells = []
            for idx in row:
                if idx is None:
                    cells.append(" " * _TF_COL_WIDTH)
                    continue
                tf_tuple = TIMEFRAMES[idx - 1]
                mt4_p = tf_tuple[0]
                secs = tf_tuple[1]
                full_label = tf_tuple[2]            # e.g. "M1  - 1 minute"
                short_label = full_label.split()[0]   # e.g. "M1"
                is_current = (mt4_p == PERIOD and secs == PERIOD_SECONDS)
                cell_color = Colors.GREEN if is_current else Colors.CYAN
                # Identical layout to Stage 2: "XX - SSSSS - LLLL" = 17 visible chars
                cells.append(f"{cell_color}{idx:>2}{Colors.RESET} - {secs:<5} - {short_label:<4}")
            print(f"  {cells[0]}{_TF_GAP}{cells[1]}{_TF_GAP}{cells[2]}")
        print(f"  {Colors.DIM}{'═' * 55}{Colors.RESET}")
        print(f"  {Colors.DIM}(green = current){Colors.RESET}")
        while True:
            try:
                tf_choice = input(f"{Colors.CYAN}  Choose 1-14 (default=1 for M1): {Colors.RESET}").strip()
                if not tf_choice:
                    return TIMEFRAMES[0]
                tf_idx = int(tf_choice) - 1
                if 0 <= tf_idx < len(TIMEFRAMES):
                    return TIMEFRAMES[tf_idx]
                print(f"{Colors.RED}  Invalid choice (1-14).{Colors.RESET}")
            except (ValueError, KeyboardInterrupt):
                print(f"{Colors.RED}  Invalid input.{Colors.RESET}")
            except EOFError:
                return TIMEFRAMES[0]

    def _choose_history_days():
        # للفريمات الفرعية: لا نسأل، نُرجع قيمة ثابتة (نفس سلوك Initial Setup)
        if PERIOD_SECONDS < 60:
            _auto_days = (SUBSECOND_HISTORY_CANDLES * PERIOD_SECONDS) / 86400.0 * 1.15
            print(f"{Colors.DIM}  Sub-second timeframe: auto history ({SUBSECOND_HISTORY_CANDLES} candles){Colors.RESET}")
            return _auto_days, SUBSECOND_HISTORY_CANDLES
        # للفريمات 60s+: نسأل المستخدم
        print(f"{Colors.CYAN}{'═'*70}{Colors.RESET}")
        print(f"{Colors.BOLD}  Select History Size:{Colors.RESET}")
        print(f"{Colors.CYAN}{'═'*70}{Colors.RESET}")
        _default_days_local = (1000 * PERIOD_SECONDS) / 86400.0 * 1.15  # +15% margin
        print(f"  {Colors.CYAN}Enter number of days (e.g., 1, 2, 3, 4...)")
        print(f"   Or press Enter for default 1000 candles ({_default_days_local:.2f} days){Colors.RESET}")
        print(f"{Colors.CYAN}{'═'*70}{Colors.RESET}")
        while True:
            try:
                days_input = input(f"{Colors.CYAN}Days: {Colors.RESET}").strip()
                if not days_input:
                    return _default_days_local, 1000
                days_val = float(days_input)
                if days_val <= 0:
                    print(f"{Colors.RED}Must be positive.{Colors.RESET}")
                    continue
                # Apply 15% margin to user-specified days too
                days_with_margin = days_val * 1.15
                candles = max(int((days_val * 86400) / PERIOD_SECONDS), 100)
                return days_with_margin, candles
            except (ValueError, KeyboardInterrupt):
                print(f"{Colors.RED}Invalid input.{Colors.RESET}")
            except EOFError:
                return _default_days_local, 1000

    # status update loop with interactive menu support
    while not stop_requested:
        try:
            while True:
                if _enter_pressed.is_set() or stop_requested or reinit_requested:
                    break
                _draw_live_table(first_draw=False)
                # تحديث السعر كل تيكس: ننتظر التيكس القادم (وفallback 0.5s للحالة)
                TICK_UPDATE_EVENT.wait(timeout=STATUS_REFRESH_INTERVAL)
                TICK_UPDATE_EVENT.clear()
            if stop_requested:
                break
            if reinit_requested:
                break
            action = _show_interactive_menu()
            _enter_pressed.clear()
            if action == 4:
                _draw_live_table(first_draw=True)
                continue
            elif action == 5:
                stop_requested = True
                break
            elif action in (1, 2, 3):
                new_period = PERIOD
                new_period_seconds = PERIOD_SECONDS
                if action in (1, 3):
                    _new_tf = _choose_timeframe()
                    new_period = _new_tf[0]
                    new_period_seconds = _new_tf[1]
                new_days = HISTORY_DAYS
                new_candles = INITIAL_CANDLES
                # للفريمات الفرعية: _choose_history_days يُرجع قيمة ثابتة تلقائياً
                # للفريمات 60s+: يسأل المستخدم
                if action in (2, 3):
                    new_days, new_candles = _choose_history_days()
                # Update globals BEFORE the live re-fetch — refetch_all_candles()
                # reads IS_SUBSECOND / SUBSECOND_SECONDS to decide which stream
                # function (subsecond_stream vs realtime_stream) to restart.
                globals()['PERIOD'] = new_period
                globals()['PERIOD_SECONDS'] = new_period_seconds
                if new_period_seconds < 60:
                    globals()['IS_SUBSECOND'] = True
                    globals()['SUBSECOND_SECONDS'] = new_period_seconds
                    globals()['SUBSECOND_LABEL'] = f"{new_period_seconds}S"
                else:
                    globals()['IS_SUBSECOND'] = False
                    globals()['SUBSECOND_SECONDS'] = 0
                    globals()['SUBSECOND_LABEL'] = ""
                globals()['HISTORY_DAYS'] = new_days
                globals()['INITIAL_CANDLES'] = new_candles
                if new_period_seconds < 60 and _use_browser_history():
                    globals()['SUBSECOND_HISTORY_CANDLES'] = SUBSECOND_HISTORY_SECONDS // new_period_seconds
                    globals()['INITIAL_CANDLES'] = globals()['SUBSECOND_HISTORY_CANDLES']
                globals()['FETCH_DURATION_SECONDS'] = int(86400 * new_days) if new_days > 0 else 0
                print(f"\n{Colors.GREEN}Switching to {_current_period_label()}, {new_days:.2f} days = {new_candles} candles...{Colors.RESET}")
                # ===== LIVE SWITCH (no restart) =====
                # Instead of os.execv() (which kills the process and forces the
                # user to re-enter license / timeframe / domain / login), we
                # stop the existing streams, clear the candles for every asset,
                # re-fetch with the new timeframe and restart the streams —
                # all in-place. The status loop simply continues afterwards.
                if not (CONNECTION_ALIVE and CLIENT is not None and CLIENT.api is not None):
                    print(f"{Colors.YELLOW}  Connection is currently down — waiting for auto-reconnect to apply the new timeframe.{Colors.RESET}")
                    # The auto_reconnect loop will pick up the new globals on
                    # its next iteration and re-fetch with the new timeframe.
                else:
                    try:
                        print(f"{Colors.DIM}  Re-fetching all assets live (no restart)...{Colors.RESET}")
                        refetch_fut = asyncio.run_coroutine_threadsafe(refetch_all_candles(), ASYNC_LOOP)
                        refetch_fut.result(timeout=900)  # up to 15 minutes for 41 assets
                    except Exception as e:
                        print(f"{Colors.RED}  Live re-fetch error: {e}{Colors.RESET}")
                _enter_pressed.clear()
                _draw_live_table(first_draw=True)
                continue
        except KeyboardInterrupt:
            stop_requested = True
            break
        except Exception as e:
            log_exception("main status loop", e)
            try:
                print(f"\n{Colors.CYAN}[main loop] recovered from error: {e}{Colors.RESET}", flush=True)
            except Exception:
                pass
            time.sleep(1)

    # If reinit was requested: نُعيد تشغيل البرنامج بشكل نظيف
    # ملاحظة: هذا إعادة بناء كاملة (full re-init) لكنها أسرع من إغلاق الترمنال
    if reinit_requested:
        print(f"\n{Colors.CYAN}Applying new timeframe settings...{Colors.RESET}")
        sys.stdout.flush()
        time.sleep(1.0)
        # إعادة تنفيذ البرنامج بشكل نظيف — يبدأ من الترحيب مباشرة (لا ترخيص بعد الآن)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    # graceful shutdown
    cols, rows = _get_term_size()
    gap_lines = 3 if GAP_FILL_SUMMARY else 0
    total_lines_now = 3 + len(all_assets) + 1 + gap_lines
    sys.stdout.write(f"\033[{total_lines_now}B\n")
    sys.stdout.flush()
    print(f"{Colors.RED}Stopped{Colors.RESET}")
    print(f"{Colors.CYAN}Total updates: {sum(a.updates for a in all_assets):,}{Colors.RESET}")
    print(f"{Colors.CYAN}Total candles stored: {sum(len(a.candles) for a in all_assets):,}{Colors.RESET}")
    print()
    # (تم نزع MT4) — writer_thread أُزيل؛ لا شيء لإيقافه هنا
