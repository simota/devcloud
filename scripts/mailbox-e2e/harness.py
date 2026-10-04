#!/usr/bin/env python3
"""Black-box acceptance harness for devcloud-mailbox.

Oracles: the spec (acceptance-criteria.md + ADR-001 incl. Amendment 1,
p3-precedent-voice.json) and a live mailhog/mailhog:v1.0.1 container.
Implementation sources are never read.  Python 3 stdlib only.

Output: one "[PASS]/[FAIL]/[SKIP]/[INFO] AC-xx description" line per check and
a summary line.  Exit 1 when any check fails.  Mail bodies and credentials are
never printed (only ids, sizes, counts, header names and short header values).
"""

import argparse
import base64
import hashlib
import json
import os
import quopri
import random
import re
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

# --------------------------------------------------------------------------
# reporting

COUNTS = {"PASS": 0, "FAIL": 0, "SKIP": 0, "INFO": 0}
FAILED = []
SLOW = False


def emit(status, ac, desc, detail=""):
    line = "[%s] %s %s" % (status, ac, desc)
    if detail:
        line += " -- " + detail
    print(line, flush=True)
    COUNTS[status] += 1
    if status == "FAIL":
        FAILED.append("%s %s" % (ac, desc))


def check(ac, desc, ok, detail=""):
    emit("PASS" if ok else "FAIL", ac, desc, "" if ok else detail)
    return bool(ok)


def info(ac, desc, detail=""):
    emit("INFO", ac, desc, detail)


def skip(ac, desc, detail=""):
    emit("SKIP", ac, desc, detail)


class group:
    """Turns an unexpected exception inside a block into one FAIL line."""

    def __init__(self, ac, desc):
        self.ac, self.desc = ac, desc

    def __enter__(self):
        return self

    def __exit__(self, et, ev, tb):
        if et is None or et is KeyboardInterrupt:
            return False
        emit("FAIL", self.ac, self.desc + " (aborted)", "%s: %s" % (et.__name__, str(ev)[:200]))
        return True


def safe_val(v, path):
    if v is None or isinstance(v, (bool, int, float)):
        return repr(v)
    if isinstance(v, str):
        if any(k in path for k in ("Body", "Data", "body", "text", "snippet")):
            return "str(len=%d)" % len(v)
        return repr(v[:80])
    return type(v).__name__


def tname(v):
    if v is None:
        return "null"
    return {bool: "bool", int: "number", float: "number", str: "string", list: "array", dict: "object"}.get(type(v), type(v).__name__)


def first_diff(a, b, path="$"):
    """Describe the first difference (ours=a, oracle=b) without printing bodies."""
    if tname(a) != tname(b):
        return "%s: type %s vs %s" % (path, tname(a), tname(b))
    if isinstance(a, dict):
        ka, kb = set(a), set(b)
        if ka != kb:
            return "%s: keys only-ours=%s only-oracle=%s" % (path, sorted(ka - kb)[:8], sorted(kb - ka)[:8])
        for k in sorted(a):
            d = first_diff(a[k], b[k], "%s.%s" % (path, k))
            if d:
                return d
        return None
    if isinstance(a, list):
        if len(a) != len(b):
            return "%s: len %d vs %d" % (path, len(a), len(b))
        for i, (x, y) in enumerate(zip(a, b)):
            d = first_diff(x, y, "%s[%d]" % (path, i))
            if d:
                return d
        return None
    if a != b:
        return "%s: %s vs %s" % (path, safe_val(a, path), safe_val(b, path))
    return None


# --------------------------------------------------------------------------
# raw sockets: HTTP/1.1 client, SSE client, SMTP client


class SockReader:
    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def _fill(self):
        d = self.sock.recv(262144)
        if not d:
            raise EOFError("connection closed")
        self.buf += d

    def readline(self, limit=8 << 20):
        while b"\n" not in self.buf:
            if len(self.buf) > limit:
                raise ValueError("line too long")
            self._fill()
        i = self.buf.index(b"\n") + 1
        line, self.buf = self.buf[:i], self.buf[i:]
        return line

    def read(self, n):
        while len(self.buf) < n:
            self._fill()
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def read_all(self):
        try:
            while True:
                self._fill()
        except EOFError:
            pass
        out, self.buf = self.buf, b""
        return out

    def read_head(self, limit=1 << 20):
        while b"\r\n\r\n" not in self.buf:
            if len(self.buf) > limit:
                raise ValueError("response head too large")
            self._fill()
        i = self.buf.index(b"\r\n\r\n")
        head, self.buf = self.buf[:i], self.buf[i + 4:]
        return head


class Resp:
    def __init__(self, status, reason, headers, body, head, malformed):
        self.status = status
        self.reason = reason
        self.headers = headers  # list of (name, value)
        self.body = body
        self.head = head
        self.malformed = malformed  # header lines without a colon

    def header(self, name):
        for k, v in self.headers:
            if k.lower() == name.lower():
                return v
        return None

    def header_all(self, name):
        return [v for k, v in self.headers if k.lower() == name.lower()]

    def has(self, name):
        return self.header(name) is not None

    def json(self):
        return json.loads(self.body.decode("utf-8"))

    def ctype(self):
        return (self.header("Content-Type") or "").strip()

    def bare_ctype(self):
        return self.ctype().split(";")[0].strip().lower()


def parse_head(head):
    lines = head.split(b"\r\n")
    status_line = lines[0].decode("latin-1")
    parts = status_line.split(" ", 2)
    status = int(parts[1])
    reason = parts[2] if len(parts) > 2 else ""
    headers, malformed = [], []
    for raw in lines[1:]:
        s = raw.decode("latin-1")
        if ":" not in s or s[:1] in (" ", "\t"):
            malformed.append(s)
            continue
        k, v = s.split(":", 1)
        headers.append((k.strip(), v.strip()))
    return status, reason, headers, malformed


def read_chunked(r):
    out = b""
    while True:
        size_line = r.readline().strip()
        size = int(size_line.split(b";")[0], 16)
        if size == 0:
            # trailers
            while r.readline() not in (b"\r\n", b"\n", b""):
                pass
            return out
        out += r.read(size)
        r.read(2)


def http_request(port, method, path, headers=None, body=b"", host=None, timeout=30.0):
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    try:
        lines = ["%s %s HTTP/1.1" % (method, path),
                 "Host: %s" % (host if host is not None else "127.0.0.1:%d" % port),
                 "Connection: close"]
        for k, v in (headers or []):
            lines.append("%s: %s" % (k, v))
        if body or method in ("POST", "PUT"):
            lines.append("Content-Length: %d" % len(body))
        sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + body)
        r = SockReader(sock)
        head = r.read_head()
        status, reason, hdrs, malformed = parse_head(head)
        resp = Resp(status, reason, hdrs, b"", head, malformed)
        te = (resp.header("Transfer-Encoding") or "").lower()
        cl = resp.header("Content-Length")
        if method == "HEAD" or status in (204, 304) or 100 <= status < 200:
            body_bytes = b""
        elif "chunked" in te:
            body_bytes = read_chunked(r)
        elif cl is not None:
            body_bytes = r.read(int(cl))
        else:
            body_bytes = r.read_all()
        resp.body = body_bytes
        return resp
    finally:
        sock.close()


def status_of(ep, method, path):
    """HTTP status, or -1 when the server drops the connection without a response."""
    try:
        return ep.req(method, path).status
    except (OSError, EOFError, ValueError):
        return -1


def basic(user, password):
    return "Basic " + base64.b64encode(("%s:%s" % (user, password)).encode()).decode()


OBSERVED = []  # (label, method, path, status, headers) for every response from OUR server


class Endpoint:
    def __init__(self, label, http_port, smtp_port, ours, creds=None):
        self.label = label
        self.http = http_port
        self.smtp = smtp_port
        self.ours = ours
        self.creds = creds

    def req(self, method, path, headers=None, host=None, timeout=30.0, auth=True, body=b""):
        h = list(headers or [])
        if auth and self.creds:
            h.append(("Authorization", basic(*self.creds)))
        r = http_request(self.http, method, path, headers=h, host=host, timeout=timeout, body=body)
        if self.ours:
            OBSERVED.append((self.label, method, path.split("?")[0], r.status, list(r.headers)))
        return r

    def get(self, path, **kw):
        return self.req("GET", path, **kw)

    def getj(self, path, **kw):
        r = self.get(path, **kw)
        obj = None
        if r.status == 200 and r.body:
            obj = json.loads(r.body.decode("utf-8"))
        return r, obj


class SSEClient:
    def __init__(self, ep, path="/api/v1/events", host=None, headers=None, auth=True, rcvbuf=None, read=True, timeout=10.0):
        self.ep = ep
        self.events = []  # (monotonic t, data string, number of data lines)
        self.comments = []  # (t, line)
        self.fields = []  # (t, line) non-data fields e.g. MailHog "keepalive:"
        self.closed_at = None
        self.cond = threading.Condition()
        self.bytes_read = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if rcvbuf:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        self.sock.settimeout(timeout)
        self.sock.connect(("127.0.0.1", ep.http))
        lines = ["GET %s HTTP/1.1" % path, "Host: %s" % (host if host is not None else "127.0.0.1:%d" % ep.http),
                 "Accept: text/event-stream"]
        h = list(headers or [])
        if auth and ep.creds:
            h.append(("Authorization", basic(*ep.creds)))
        for k, v in h:
            lines.append("%s: %s" % (k, v))
        self.sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))
        self.r = SockReader(self.sock)
        head = self.r.read_head()
        self.status, _, self.headers, _ = parse_head(head)
        self.resp = Resp(self.status, "", self.headers, b"", head, [])
        if ep.ours:
            OBSERVED.append((ep.label, "GET", path.split("?")[0], self.status, list(self.headers)))
        self.chunked = "chunked" in (self.resp.header("Transfer-Encoding") or "").lower()
        self.thread = None
        if self.status != 200:
            self.close()
        elif read:
            self.start()

    def start(self):
        self.sock.settimeout(None)
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _chunks(self):
        if self.chunked:
            while True:
                size = int(self.r.readline().strip().split(b";")[0], 16)
                if size == 0:
                    return
                yield self.r.read(size)
                self.r.read(2)
        else:
            if self.r.buf:
                b, self.r.buf = self.r.buf, b""
                yield b
            while True:
                d = self.sock.recv(262144)
                if not d:
                    return
                yield d

    def _run(self):
        pending = b""
        data_lines = []
        try:
            for chunk in self._chunks():
                self.bytes_read += len(chunk)
                pending += chunk
                while b"\n" in pending:
                    i = pending.index(b"\n")
                    line, pending = pending[:i].rstrip(b"\r").decode("utf-8", "replace"), pending[i + 1:]
                    now = time.monotonic()
                    with self.cond:
                        if line == "":
                            if data_lines:
                                self.events.append((now, "\n".join(data_lines), len(data_lines)))
                                data_lines = []
                        elif line.startswith(":"):
                            self.comments.append((now, line))
                        elif line.startswith("data:"):
                            v = line[5:]
                            data_lines.append(v[1:] if v.startswith(" ") else v)
                        else:
                            self.fields.append((now, line))
                        self.cond.notify_all()
        except (OSError, EOFError, ValueError):
            pass
        with self.cond:
            self.closed_at = time.monotonic()
            self.cond.notify_all()

    def wait(self, pred, timeout):
        deadline = time.monotonic() + timeout
        with self.cond:
            while True:
                res = pred(self)
                if res:
                    return res
                if self.closed_at is not None:
                    return None
                left = deadline - time.monotonic()
                if left <= 0:
                    return None
                self.cond.wait(left)

    def event_for(self, fixture, timeout):
        def pred(c):
            for t, data, n in c.events:
                try:
                    obj = json.loads(data)
                except ValueError:
                    continue
                if fixture_name(obj) == fixture:
                    return (t, obj, n, data)
            return None
        return self.wait(pred, timeout)

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def dot_stuff(data):
    return b"\r\n".join((b"." + l) if l.startswith(b".") else l for l in data.split(b"\r\n"))


class Smtp:
    def __init__(self, port, timeout=30.0):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.r = SockReader(self.sock)
        self.banner = self.reply()

    def reply(self):
        lines = []
        while True:
            l = self.r.readline().decode("latin-1").rstrip("\r\n")
            lines.append(l)
            if len(l) < 4 or l[3] != "-":
                break
        try:
            code = int(lines[-1][:3])
        except ValueError:
            code = 0
        return code, lines

    def cmd(self, line):
        if isinstance(line, str):
            line = line.encode("latin-1")
        self.sock.sendall(line + b"\r\n")
        return self.reply()

    def data(self, payload):
        code, _ = self.cmd(b"DATA")
        if code != 354:
            return code
        self.sock.sendall(dot_stuff(payload) + b"\r\n.\r\n")
        return self.reply()[0]

    def transaction(self, mail_from, rcpts, payload, params=""):
        code, _ = self.cmd("MAIL FROM:<%s>%s" % (mail_from, (" " + params) if params else ""))
        if code != 250:
            return code
        for rc in rcpts:
            code, _ = self.cmd("RCPT TO:<%s>" % rc)
            if code != 250:
                return code
        return self.data(payload)

    def quit(self):
        try:
            self.cmd("QUIT")
        except (OSError, EOFError):
            pass
        try:
            self.sock.close()
        except OSError:
            pass


FIX_HELO = "fixture-client.test"
FIX_FROM = "envelope-sender@bounce.test"
FIX_RCPTS = ["bob@example.test", "carol@rcpt.test"]


def send_mail(ep, payload, helo=FIX_HELO, mail_from=FIX_FROM, rcpts=FIX_RCPTS, params="", timeout=30.0):
    s = Smtp(ep.smtp, timeout=timeout)
    try:
        s.cmd("EHLO " + helo)
        return s.transaction(mail_from, rcpts, payload, params)
    finally:
        s.quit()


def send_bulk(ep, payloads, per_session=200, helo="bulk.test", mail_from="bulk@bounce.test", rcpts=("bulk@rcpt.test",)):
    codes = []
    for i in range(0, len(payloads), per_session):
        s = Smtp(ep.smtp, timeout=120)
        try:
            s.cmd("EHLO " + helo)
            for p in payloads[i:i + per_session]:
                codes.append(s.transaction(mail_from, list(rcpts), p))
        finally:
            s.quit()
    return codes


# --------------------------------------------------------------------------
# fixtures (known bytes; expectations computed here from the source text)

CRLF = b"\r\n"
BODY_MARKER = "SECRET-BODY-MARKER-91c2"
PDF_BYTES = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n" + bytes(range(256)) + b"\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"
PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")
EVIL_HTML = b"<html><body><script>alert(document.domain)</script>evil</body></html>"
EVIL_SVG = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'
ATTACH_ALLOWLIST = {"image/png", "image/jpeg", "image/gif", "image/webp", "application/pdf", "text/plain",
                    "application/zip", "application/octet-stream", "text/csv", "application/json"}


def build(*parts):
    return CRLF.join(p if isinstance(p, bytes) else p.encode("ascii") for p in parts)


def b64lines(data):
    s = base64.b64encode(data).decode()
    return [s[i:i + 76] for i in range(0, len(s), 76)] or [""]


def enc_b(text, charset, codec=None):
    return "=?%s?B?%s?=" % (charset, base64.b64encode(text.encode(codec or charset)).decode())


def enc_q(text, charset, codec=None):
    out = []
    for b in text.encode(codec or charset):
        c = chr(b)
        if c == " ":
            out.append("_")
        elif b < 128 and c.isalnum():
            out.append(c)
        else:
            out.append("=%02X" % b)
    return "=?%s?Q?%s?=" % (charset, "".join(out))


def hdr(name, subject, from_hdr="Alice Example <alice@example.test>", to_hdr="Bob <bob@example.test>",
        msgid=True, extra=()):
    out = ["From: " + from_hdr, "To: " + to_hdr, "Subject: " + subject,
           "Date: Sun, 04 Oct 2026 10:00:00 +0900"]
    if msgid:
        out.append("Message-ID: <%s@fixture.test>" % name)
    out.append("X-Fixture: " + name)
    out.append("MIME-Version: 1.0")
    out.extend(extra)
    return out


def fx(name, data, diff=True, **exp):
    d = {"name": name, "data": data, "diff": diff, "synth_msgid": b"\r\nMessage-ID:" not in data,
         "multipart": bool(re.search(rb"^Content-Type:\s*multipart/", data.split(b"\r\n\r\n")[0], re.I | re.M)),
         "sevenbit": all(b < 128 for b in data)}
    d.update(exp)
    return d


def build_fixtures():
    F = []
    # plain ASCII (with a dot-stuffed line)
    F.append(fx("plain", build(*hdr("plain", "Plain ASCII fixture"),
                               "Content-Type: text/plain; charset=us-ascii", "Content-Transfer-Encoding: 7bit", "",
                               "Hello plain world. " + BODY_MARKER, ".leading dot line", "Last line."),
                subject="Plain ASCII fixture", from_name="Alice Example", from_addr="alice@example.test",
                text=["Hello plain world.", ".leading dot line", "Last line."], warnings=False, has_html=False))
    # no Message-ID: MailHog synthesizes one
    F.append(fx("nomsgid", build(*hdr("nomsgid", "No Message-ID fixture", msgid=False),
                                 "Content-Type: text/plain", "", "no message id"),
                subject="No Message-ID fixture", text=["no message id"], warnings=False))
    # header edge cases: duplicate keys, folding, odd key case
    F.append(fx("headers-edge", build(*hdr("headers-edge", "Header edge fixture"),
                                      "X-Dup: one", "X-Dup: two", "X-Folded: part one", "\tpart two",
                                      "x-lower-case: v", "Content-Type: text/plain", "", "headers edge body"),
                subject="Header edge fixture", text=["headers edge body"]))
    # quoted-printable with =3D and a soft line break
    qp_body = b"a=3Db caf=C3=A9 soft=\r\nbreak end"
    F.append(fx("qp", build(*hdr("qp", "QP fixture"), "Content-Type: text/plain; charset=utf-8",
                            "Content-Transfer-Encoding: quoted-printable", "", qp_body),
                subject="QP fixture", text=[quopri.decodestring(qp_body).decode("utf-8")], warnings=False,
                raw_contains="=3D"))
    # base64 UTF-8 + RFC2047 B subject
    t = "こんにちは世界 UTF-8 base64 本文\r\n二行目です"
    F.append(fx("b64-utf8", build(*hdr("b64-utf8", enc_b("Base64 件名テスト", "UTF-8", "utf-8")),
                                  "Content-Type: text/plain; charset=UTF-8", "Content-Transfer-Encoding: base64", "",
                                  *b64lines(t.encode("utf-8"))),
                subject="Base64 件名テスト", text=["こんにちは世界 UTF-8 base64 本文", "二行目です"], warnings=False))
    # ISO-2022-JP body (7bit) + RFC2047 B subject
    t = "これはISO-2022-JPの本文です。\r\n漢字とカタカナ"
    F.append(fx("iso2022jp", build(*hdr("iso2022jp", enc_b("日本語の件名テスト", "ISO-2022-JP", "iso2022_jp")),
                                   "Content-Type: text/plain; charset=ISO-2022-JP", "Content-Transfer-Encoding: 7bit",
                                   "", t.encode("iso2022_jp")),
                subject="日本語の件名テスト", text=["これはISO-2022-JPの本文です。", "漢字とカタカナ"], warnings=False))
    # RFC2047 Q: display name + two adjacent encoded-words in Subject
    F.append(fx("rfc2047-q", build(*hdr("rfc2047-q", enc_q("Café ☕", "UTF-8", "utf-8") + " " + enc_q(" menü", "UTF-8", "utf-8"),
                                       from_hdr=enc_q("Jürgen Müller", "UTF-8", "utf-8") + " <jurgen@example.test>"),
                                   "Content-Type: text/plain; charset=us-ascii", "", "q header body"),
                subject="Café ☕ menü", from_name="Jürgen Müller", from_addr="jurgen@example.test",
                text=["q header body"], warnings=False))
    # Shift_JIS 8bit (0x5C trail bytes: ソ表) + half-width kana
    t = "シフトJISの本文：ソ表能テスト\r\n半角ｶﾀｶﾅ"
    F.append(fx("sjis-8bit", build(*hdr("sjis-8bit", enc_b("SJIS件名テスト", "Shift_JIS", "shift_jis")),
                                   "Content-Type: text/plain; charset=Shift_JIS", "Content-Transfer-Encoding: 8bit",
                                   "", t.encode("shift_jis")),
                diff="info", subject="SJIS件名テスト", text=["シフトJISの本文：ソ表能テスト", "半角ｶﾀｶﾅ"], warnings=False))
    # EUC-JP 8bit
    t = "EUC-JPの本文です。漢字テスト"
    F.append(fx("eucjp-8bit", build(*hdr("eucjp-8bit", enc_b("EUC件名テスト", "EUC-JP", "euc_jp")),
                                    "Content-Type: text/plain; charset=EUC-JP", "Content-Transfer-Encoding: 8bit",
                                    "", t.encode("euc_jp")),
                diff="info", subject="EUC件名テスト", text=[t], warnings=False))
    # strict ISO-8859-1 incl. 0x80-0x9F (WHATWG windows-1252 would map 0x80 to EURO SIGN)
    t = "Grüße café © ctrl:" + "".join(chr(c) for c in (0x80, 0x85, 0x8A, 0x9F)) + " end"
    F.append(fx("latin1", build(*hdr("latin1", enc_q("Grüße aus Köln", "ISO-8859-1", "latin-1")),
                                "Content-Type: text/plain; charset=ISO-8859-1",
                                "Content-Transfer-Encoding: quoted-printable", "",
                                quopri.encodestring(t.encode("latin-1")).rstrip(b"\n").replace(b"\n", b"\r\n")),
                subject="Grüße aus Köln", text=[t], warnings=False))
    # multipart/alternative
    F.append(fx("alternative", build(*hdr("alternative", "Alternative fixture"),
                                     'Content-Type: multipart/alternative; boundary="alt-boundary-1"', "",
                                     "--alt-boundary-1", "Content-Type: text/plain; charset=utf-8",
                                     "Content-Transfer-Encoding: quoted-printable", "", "Plain alternative caf=C3=A9",
                                     "--alt-boundary-1", "Content-Type: text/html; charset=utf-8",
                                     "Content-Transfer-Encoding: quoted-printable", "",
                                     "<p>HTML alternative caf=C3=A9</p>", "--alt-boundary-1--"),
                subject="Alternative fixture", text=["Plain alternative café"], has_html=True, warnings=False,
                attachments=[]))
    # multipart/mixed + PDF with RFC2231 Japanese filename
    fname = "請求書.pdf"
    F.append(fx("mixed-pdf", build(*hdr("mixed-pdf", "Mixed PDF fixture"),
                                   'Content-Type: multipart/mixed; boundary="mix-boundary-1"', "",
                                   "--mix-boundary-1", "Content-Type: text/plain; charset=utf-8", "",
                                   "See attached invoice.", "--mix-boundary-1",
                                   'Content-Type: application/pdf; name="invoice.pdf"',
                                   "Content-Disposition: attachment; filename*=UTF-8''" + urllib.parse.quote(fname.encode("utf-8")),
                                   "Content-Transfer-Encoding: base64", "", *b64lines(PDF_BYTES), "--mix-boundary-1--"),
                subject="Mixed PDF fixture", text=["See attached invoice."], warnings=False,
                attachments=[(fname, "application/pdf", PDF_BYTES)]))
    # nested: mixed > (alternative(plain, html), png attachment)
    F.append(fx("nested", build(*hdr("nested", "Nested fixture"),
                                'Content-Type: multipart/mixed; boundary="outer-1"', "",
                                "--outer-1", 'Content-Type: multipart/alternative; boundary="inner-1"', "",
                                "--inner-1", "Content-Type: text/plain; charset=utf-8", "", "Nested plain body",
                                "--inner-1", "Content-Type: text/html; charset=utf-8", "", "<b>Nested html body</b>",
                                "--inner-1--", "", "--outer-1",
                                'Content-Type: image/png; name="dot.png"',
                                'Content-Disposition: attachment; filename="dot.png"',
                                "Content-Transfer-Encoding: base64", "", *b64lines(PNG_BYTES), "--outer-1--"),
                subject="Nested fixture", text=["Nested plain body"], has_html=True, warnings=False,
                attachments=[("dot.png", "image/png", PNG_BYTES)]))
    # preamble + terminator + epilogue
    F.append(fx("preamble", build(*hdr("preamble", "Preamble fixture"),
                                  'Content-Type: multipart/mixed; boundary="pre-1"', "",
                                  "This is a multi-part message in MIME format.", "--pre-1",
                                  "Content-Type: text/plain; charset=us-ascii", "", "Part one text.", "--pre-1",
                                  'Content-Type: text/plain; name="notes.txt"',
                                  'Content-Disposition: attachment; filename="notes.txt"', "",
                                  "notes attachment body", "--pre-1--", "Epilogue text that is not a part."),
                subject="Preamble fixture", text=["Part one text."],
                text_absent=["multi-part message in MIME format", "Epilogue text"], warnings=False,
                attachments=[("notes.txt", "text/plain", b"notes attachment body")]))
    # missing terminator
    F.append(fx("noterm", build(*hdr("noterm", "Missing terminator fixture"),
                                'Content-Type: multipart/mixed; boundary="nt-1"', "",
                                "--nt-1", "Content-Type: text/plain", "", "first part text",
                                "--nt-1", "Content-Type: text/plain", "", "second part unterminated"),
                diff="info", subject="Missing terminator fixture", text=["first part text"], warnings=True))
    # multipart without boundary
    F.append(fx("noboundary", build(*hdr("noboundary", "No boundary fixture"), "Content-Type: multipart/mixed", "",
                                    "just text without boundary"),
                diff="info", subject="No boundary fixture", text=["just text without boundary"], warnings=True))
    # HTML with <script> and a link
    F.append(fx("html-script", build(*hdr("html-script", "HTML script fixture"),
                                     "Content-Type: text/html; charset=utf-8", "",
                                     '<html><head><title>t</title></head><body><p>Hello <a href="https://example.test/landing">landing link</a></p>',
                                     "<script>document.title='pwned'</script></body></html>"),
                subject="HTML script fixture", has_html=True, warnings=False,
                html_contains=['href="https://example.test/landing"']))
    # attachments declared text/html and image/svg+xml
    F.append(fx("att-unsafe", build(*hdr("att-unsafe", "Unsafe attachment fixture"),
                                    'Content-Type: multipart/mixed; boundary="ua-1"', "",
                                    "--ua-1", "Content-Type: text/plain", "", "two unsafe attachments", "--ua-1",
                                    'Content-Type: text/html; name="evil.html"',
                                    'Content-Disposition: attachment; filename="evil.html"',
                                    "Content-Transfer-Encoding: base64", "", *b64lines(EVIL_HTML), "--ua-1",
                                    'Content-Type: image/svg+xml; name="evil.svg"',
                                    'Content-Disposition: attachment; filename="evil.svg"',
                                    "Content-Transfer-Encoding: base64", "", *b64lines(EVIL_SVG), "--ua-1--"),
                subject="Unsafe attachment fixture", text=["two unsafe attachments"], warnings=False,
                attachments=[("evil.html", "text/html", EVIL_HTML), ("evil.svg", "image/svg+xml", EVIL_SVG)]))
    # attachment whose decoded filename contains CR/LF (header injection attempt)
    F.append(fx("att-crlf", build(*hdr("att-crlf", "CRLF filename fixture"),
                                  'Content-Type: multipart/mixed; boundary="crlf-1"', "",
                                  "--crlf-1", "Content-Type: text/plain", "", "crlf filename body", "--crlf-1",
                                  'Content-Type: text/plain; name="%s"' % enc_b("a\r\nX-Injected: 1.txt", "UTF-8", "utf-8"),
                                  "Content-Disposition: attachment; filename*=UTF-8''evil%0D%0AX-Injected%3A%20yes.txt",
                                  "", "injected attachment body", "--crlf-1--"),
                subject="CRLF filename fixture", crlf=True))
    # unknown charset / broken encodings
    F.append(fx("unknown-charset", build(*hdr("unknown-charset", "Unknown charset fixture"),
                                         "Content-Type: text/plain; charset=x-unknown-zz", "", "unknown charset body"),
                subject="Unknown charset fixture", warnings=True))
    F.append(fx("bad-b64", build(*hdr("bad-b64", "Broken base64 fixture"), "Content-Type: text/plain; charset=utf-8",
                                 "Content-Transfer-Encoding: base64", "", "SGVsbG8@@@!!!#", "V29y*bGQ===x"),
                subject="Broken base64 fixture", warnings=True))
    F.append(fx("bad-qp", build(*hdr("bad-qp", "Broken QP fixture"), "Content-Type: text/plain; charset=utf-8",
                                "Content-Transfer-Encoding: quoted-printable", "", "bad =ZZ escape and trailing =4"),
                subject="Broken QP fixture", warnings=True))
    return F


def hget(headers, name):
    if not isinstance(headers, dict):
        return None
    for k, v in headers.items():
        if k.lower() == name.lower():
            return v
    return None


def fixture_name(msg):
    try:
        v = hget(msg["Content"]["Headers"], "X-Fixture")
        return v[0] if v else None
    except (KeyError, TypeError):
        return None


def qid(i):
    return urllib.parse.quote(i, safe="")


# --------------------------------------------------------------------------
# normalization for DIFF


def id_sub(s, ids):
    for i in ids:
        if i:
            s = re.sub(r"(?<![A-Za-z0-9])%s(?![A-Za-z0-9])" % re.escape(i), "<ID>", s)
    return s


def norm_msg(m, ids, synth_msgid=False, drop_helo=False):
    def walk(x):
        if isinstance(x, dict):
            return {k: walk(v) for k, v in x.items()}
        if isinstance(x, list):
            return [walk(v) for v in x]
        if isinstance(x, str):
            return id_sub(x, ids)
        return x
    out = walk(m)
    if isinstance(out, dict):
        if isinstance(out.get("Created"), str):
            out["Created"] = "<CREATED>"
        hdrs = (out.get("Content") or {}).get("Headers")
        if isinstance(hdrs, dict):
            for k in list(hdrs):
                vals = hdrs[k]
                if k.lower() == "received" and isinstance(vals, list):
                    hdrs[k] = [norm_received(v, drop_helo) if isinstance(v, str) else v for v in vals]
                elif k.lower() == "message-id" and synth_msgid:
                    hdrs[k] = ["<SYNTH-MSGID>"]
        if drop_helo and isinstance(out.get("Raw"), dict):
            out["Raw"]["Helo"] = "<HELO>"
    return out


def norm_received(v, drop_helo=False):
    v = re.sub(r";[^;]*$", "; <DATE>", v)
    if drop_helo:
        v = re.sub(r"^from .*? by ", "from <HELO> by ", v, flags=re.S)
    return v


def norm_header_block(block, ids, synth_msgid):
    """Unfold a header block into a sorted multiset of (lower name, normalized value)."""
    logical = []
    for line in block.split("\r\n"):
        if line[:1] in (" ", "\t") and logical:
            logical[-1] += "\r\n" + line
        elif line:
            logical.append(line)
    out = []
    for l in logical:
        k, _, v = l.partition(":")
        v = id_sub(v.strip(), ids)
        if k.strip().lower() == "received":
            v = norm_received(v)
        if k.strip().lower() == "message-id" and synth_msgid:
            v = "<SYNTH-MSGID>"
        out.append((k.strip().lower(), v))
    return sorted(out)


def cd_filename(v):
    if not v:
        return None
    m = re.search(r"filename\*\s*=\s*([^']*)'[^']*'([^;\s]+)", v, re.I)
    if m:
        cs = m.group(1) or "utf-8"
        try:
            return urllib.parse.unquote(m.group(2), encoding=cs, errors="strict")
        except (LookupError, UnicodeDecodeError):
            return urllib.parse.unquote(m.group(2), errors="replace")
    m = re.search(r'filename\s*=\s*"((?:[^"\\]|\\.)*)"', v, re.I)
    if m:
        return re.sub(r"\\(.)", r"\1", m.group(1))
    m = re.search(r"filename\s*=\s*([^;]+)", v, re.I)
    return m.group(1).strip() if m else None


def header_injection_problems(resp):
    probs = []
    if resp.malformed:
        probs.append("%d malformed header line(s)" % len(resp.malformed))
    for k, v in resp.headers:
        if k.lower().startswith("x-injected"):
            probs.append("injected header %s" % k)
        if any(ord(c) < 32 and c != "\t" for c in v):
            probs.append("CTL in %s" % k)
    return probs


# --------------------------------------------------------------------------
# message maps


def v1_map(ep):
    r, arr = ep.getj("/api/v1/messages")
    out = {}
    for m in arr or []:
        n = fixture_name(m)
        if n is not None:
            out[n] = m
    return out


def delete_all(ep):
    return ep.req("DELETE", "/api/v1/messages")


def send_fixtures(ep, fixtures):
    codes = {}
    for f in fixtures:
        codes[f["name"]] = send_mail(ep, f["data"])
    return codes


RFC3339 = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|[+-]\d\d:\d\d)$")


def path_obj(addr):
    mb, _, dom = addr.partition("@")
    return {"Relays": None, "Mailbox": mb, "Domain": dom, "Params": ""}


# --------------------------------------------------------------------------
# DIFF + compat suite (MailHog API)


def go_atoi(s):
    if s is None:
        return None
    if re.fullmatch(r"[+-]?[0-9]+", s):
        v = int(s)
        if -(1 << 63) <= v < (1 << 63):
            return v
    return None


def expected_page(total, query):
    qs = urllib.parse.parse_qs(query.lstrip("?"), keep_blank_values=True)
    start = go_atoi((qs.get("start") or [None])[0])
    limit = go_atoi((qs.get("limit") or [None])[0])
    start = start if start is not None and start > 0 else 0
    limit = min(limit, 250) if limit is not None and limit > 0 else 50
    count = max(0, min(limit, total - start))
    return start, limit, count


def sse_warmup(ep, client, timeout=30.0):
    """Send warm-up messages until one arrives on `client`; True once the stream is live."""
    if client is None or client.status != 200:
        return False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        name = "sse-warmup-" + secrets.token_hex(3)
        send_mail(ep, build("Subject: " + name, "X-Fixture: " + name, "", "warm-up"))
        if client.event_for(name, 2.0):
            return True
        if client.closed_at is not None:
            return False
    return False


def subjects(items):
    return [fixture_name(m) for m in items]


def compat_suite(ours, oracle, fixtures, args):
    have = oracle is not None
    nodiff = "oracle unavailable"
    fmap = {f["name"]: f for f in fixtures}

    # ---- AC-03 empty state + AC-06 delete-all
    with group("AC-03", "empty state"):
        r = delete_all(ours)
        check("AC-06", "DELETE /api/v1/messages -> 200 with empty body", r.status == 200 and r.body == b"",
              "status=%d len=%d" % (r.status, len(r.body)))
        if have:
            delete_all(oracle)
        r = ours.get("/api/v2/messages")
        ok = r.status == 200 and r.json() == {"total": 0, "count": 0, "start": 0, "items": []}
        check("AC-03", 'empty GET /api/v2/messages is {"total":0,"count":0,"start":0,"items":[]}', ok,
              "status=%d len=%d" % (r.status, len(r.body)))
        check("AC-03", "GET /api/v2/messages Content-Type text/json", r.bare_ctype() == "text/json", r.ctype())
        r1 = ours.get("/api/v1/messages")
        check("AC-06", "empty GET /api/v1/messages is []", r1.status == 200 and r1.json() == [],
              "status=%d" % r1.status)
        check("AC-06", "GET /api/v1/messages Content-Type text/json", r1.bare_ctype() == "text/json", r1.ctype())
        if have:
            o = oracle.get("/api/v2/messages")
            check("AC-03", "DIFF empty state body/content-type", (r.status, r.json(), r.bare_ctype()) == (o.status, o.json(), o.bare_ctype()))
        else:
            skip("AC-03", "DIFF empty state", nodiff)

    # ---- SSE clients opened before fixtures are sent (AC-09 DIFF)
    sse_ours = SSEClient(ours)
    sse_orc = SSEClient(oracle) if have else None
    check("AC-09", "GET /api/v1/events opens (200)", sse_ours.status == 200, "status=%d" % sse_ours.status)
    # Make sure both SSE subscriptions are live before the fixtures are sent: MailHog (amd64-emulated)
    # can attach its broker subscription after the response headers, so warm up each stream until a
    # message is delivered, then clear the warm-up messages so later list/search comparisons are unaffected.
    warm_ok = sse_warmup(ours, sse_ours)
    if have:
        if not sse_warmup(oracle, sse_orc):
            info("AC-09", "oracle SSE stream did not deliver a warm-up message within 30s")
        delete_all(oracle)
    if not warm_ok:
        info("AC-09", "ours SSE stream did not deliver a warm-up message within 30s")
    delete_all(ours)

    # ---- send fixtures
    with group("AC-04", "send fixture set F"):
        codes = send_fixtures(ours, fixtures)
        bad = {k: v for k, v in codes.items() if v != 250}
        check("AC-04", "SMTP accepts every fixture with 250 (%d fixtures)" % len(fixtures), not bad, "non-250: %s" % bad)
        if have:
            ocodes = send_fixtures(oracle, fixtures)
            info("AC-04", "oracle SMTP codes", "non-250: %s" % {k: v for k, v in ocodes.items() if v != 250})

    om = v1_map(ours)
    tm = v1_map(oracle) if have else {}
    check("AC-04", "every fixture listed by /api/v1/messages", all(f["name"] in om for f in fixtures),
          "missing: %s" % [f["name"] for f in fixtures if f["name"] not in om])

    # ---- AC-09 SSE DIFF (events for the fixtures)
    with group("AC-09", "SSE events for fixtures"):
        missing, mismatch, framing = [], [], []
        for f in fixtures:
            if f["name"] not in om:
                continue
            ev = sse_ours.event_for(f["name"], 5.0)
            if not ev:
                missing.append(f["name"])
                continue
            _, obj, nlines, _ = ev
            r, detail = ours.getj("/api/v1/messages/%s" % qid(om[f["name"]]["ID"]))
            if obj != detail:
                mismatch.append("%s: %s" % (f["name"], first_diff(obj, detail)))
            if nlines != 1:
                framing.append(f["name"])
        check("AC-09", "every fixture produced an SSE data event on ours", not missing,
              "missing: %s (events=%d stream-closed=%s)" % (missing[:6], len(sse_ours.events), sse_ours.closed_at is not None))
        check("AC-09", "SSE data JSON equals GET /api/v1/messages/{id} JSON (all fixtures)", not mismatch,
              "; ".join(mismatch[:3]))
        check("AC-09", "SSE event is one compact data: line (ADR)", not framing, "multi-line: %s" % framing[:5])
        if have:
            check("AC-09", "DIFF SSE response Content-Type / Cache-Control",
                  (sse_ours.resp.bare_ctype(), (sse_ours.resp.header("Cache-Control") or "").lower()) ==
                  (sse_orc.resp.bare_ctype(), (sse_orc.resp.header("Cache-Control") or "").lower()),
                  "ours=%s/%s oracle=%s/%s" % (sse_ours.resp.ctype(), sse_ours.resp.header("Cache-Control"),
                                               sse_orc.resp.ctype(), sse_orc.resp.header("Cache-Control")))
            diffs = []
            for f in fixtures:
                if f["diff"] is not True or f["name"] not in om:
                    continue
                a = sse_ours.event_for(f["name"], 1.0)
                b = sse_orc.event_for(f["name"], 3.0)
                if not a or not b:
                    diffs.append("%s: event missing (ours=%s oracle=%s)" % (f["name"], bool(a), bool(b)))
                    continue
                d = first_diff(norm_msg(a[1], [a[1].get("ID")], f["synth_msgid"]),
                               norm_msg(b[1], [b[1].get("ID")], f["synth_msgid"]))
                if d:
                    diffs.append("%s: %s" % (f["name"], d))
            check("AC-09", "DIFF SSE Message JSON vs MailHog (normalized)", not diffs, "; ".join(diffs[:3]))
        else:
            skip("AC-09", "DIFF SSE vs MailHog", nodiff)
    sse_ours.close()
    if sse_orc:
        sse_orc.close()

    # ---- AC-04 / AC-17 FIX on ours
    with group("AC-04", "Message JSON shape (FIX)"):
        probs = []
        for f in fixtures:
            m = om.get(f["name"])
            if not m:
                continue
            n = f["name"]
            if set(m) != {"ID", "From", "To", "Content", "Created", "MIME", "Raw"}:
                probs.append("%s keys %s" % (n, sorted(m)))
                continue
            c = m["Content"]
            if not isinstance(c, dict) or set(c) != {"Headers", "Body", "Size", "MIME"}:
                probs.append("%s Content keys" % n)
                continue
            if c["MIME"] is not None:
                probs.append("%s Content.MIME not null" % n)
            if not isinstance(m["Created"], str) or not RFC3339.match(m["Created"]):
                probs.append("%s Created not RFC3339" % n)
            if set(m["Raw"] or {}) != {"From", "To", "Data", "Helo"}:
                probs.append("%s Raw keys" % n)
            if not all(isinstance(v, list) and all(isinstance(x, str) for x in v) for v in c["Headers"].values()):
                probs.append("%s header values not string arrays" % n)
            if f["multipart"] and f["name"] not in ("noboundary",):
                if not (isinstance(m["MIME"], dict) and isinstance(m["MIME"].get("Parts"), list)):
                    probs.append("%s MIME.Parts missing" % n)
            elif not f["multipart"] and m["MIME"] is not None:
                probs.append("%s MIME should be null" % n)
            if f["sevenbit"]:
                if m["Raw"]["Data"] != f["data"].decode("ascii"):
                    probs.append("%s Raw.Data != DATA bytes (synthesized headers or altered)" % n)
                if c["Size"] != len(f["data"]):
                    probs.append("%s Size %s != %d" % (n, c["Size"], len(f["data"])))
                body = f["data"].split(b"\r\n\r\n", 1)[1].decode("ascii")
                if c["Body"] != body:
                    probs.append("%s Body is not the undecoded DATA body" % n)
            subj_raw = re.search(rb"^Subject: (.*)$", f["data"], re.M).group(1).rstrip(b"\r").decode("ascii")
            if hget(c["Headers"], "Subject") != [subj_raw]:
                probs.append("%s Subject header not undecoded" % n)
        check("AC-04", "Message keys/types, Content.MIME null, Body/Headers undecoded, Size=DATA bytes, Raw.Data=DATA",
              not probs, "; ".join(probs[:4]))
        q = om.get("qp")
        check("AC-04", "QP Body keeps '=3D'", bool(q) and "=3D" in q["Content"]["Body"])

    with group("AC-17", "envelope fields (FIX)"):
        probs = []
        for f in fixtures:
            m = om.get(f["name"])
            if not m:
                continue
            if m["From"] != path_obj(FIX_FROM):
                probs.append("%s From %s" % (f["name"], first_diff(m["From"], path_obj(FIX_FROM))))
            if m["To"] != [path_obj(a) for a in FIX_RCPTS]:
                probs.append("%s To" % f["name"])
            raw = m["Raw"]
            if raw.get("From") != FIX_FROM or raw.get("To") != FIX_RCPTS or raw.get("Helo") != FIX_HELO:
                probs.append("%s Raw From/To/Helo = %r/%r/%r" % (f["name"], raw.get("From"), raw.get("To"), raw.get("Helo")))
        check("AC-17", "From/Raw.From = MAIL FROM (not header From), To/Raw.To = RCPT TO list, Raw.Helo = EHLO name",
              not probs, "; ".join(probs[:3]))

    # ---- AC-04 / AC-06 / AC-07 / AC-08 DIFF per fixture
    if have:
        for f in fixtures:
            n = f["name"]
            a, b = om.get(n), tm.get(n)
            if not a or not b:
                check("AC-04", "DIFF [%s] present on both servers" % n, False, "ours=%s oracle=%s" % (bool(a), bool(b)))
                continue
            st = "PASS"
            ra, da = ours.getj("/api/v1/messages/%s" % qid(a["ID"]))
            rb, db = oracle.getj("/api/v1/messages/%s" % qid(b["ID"]))
            d = first_diff(norm_msg(da, [a["ID"]], f["synth_msgid"]), norm_msg(db, [b["ID"]], f["synth_msgid"])) \
                if (da is not None and db is not None) else "detail status %d/%d" % (ra.status, rb.status)
            if not d and ra.bare_ctype() != rb.bare_ctype():
                d = "content-type %s vs %s" % (ra.ctype(), rb.ctype())
            if f["diff"] is True:
                check("AC-04", "DIFF [%s] GET /api/v1/messages/{id} matches MailHog" % n, not d, d or "")
            else:
                info("AC-04", "DIFF [%s] (non-F fixture, informational)" % n, d or "identical")
            # list projections equal the detail on ours
            if da is not None and a != da:
                check("AC-06", "[%s] v1 list item equals v1 detail" % n, False, first_diff(a, da))
            # download
            ra = ours.get("/api/v1/messages/%s/download" % qid(a["ID"]))
            rb = oracle.get("/api/v1/messages/%s/download" % qid(b["ID"]))
            d = None
            if (ra.status, ra.bare_ctype()) != (rb.status, rb.bare_ctype()):
                d = "status/ctype %d %s vs %d %s" % (ra.status, ra.ctype(), rb.status, rb.ctype())
            elif id_sub(ra.header("Content-Disposition") or "", [a["ID"]]) != id_sub(rb.header("Content-Disposition") or "", [b["ID"]]):
                d = "Content-Disposition %r vs %r" % (id_sub(ra.header("Content-Disposition") or "", [a["ID"]]),
                                                      id_sub(rb.header("Content-Disposition") or "", [b["ID"]]))
            else:
                ha, _, ba = ra.body.partition(b"\r\n\r\n")
                hb, _, bb = rb.body.partition(b"\r\n\r\n")
                if ba != bb:
                    d = "body bytes differ (len %d vs %d)" % (len(ba), len(bb))
                else:
                    na = norm_header_block(ha.decode("utf-8", "replace"), [a["ID"]], f["synth_msgid"])
                    nb = norm_header_block(hb.decode("utf-8", "replace"), [b["ID"]], f["synth_msgid"])
                    if na != nb:
                        d = "header multiset differs: only-ours=%s only-oracle=%s" % (
                            [x[0] for x in na if x not in nb][:4], [x[0] for x in nb if x not in na][:4])
            if f["diff"] is True:
                check("AC-07", "DIFF [%s] /download (message/rfc822, filename, headers, body)" % n, not d, d or "")
            else:
                info("AC-07", "DIFF [%s] /download (informational)" % n, d or "identical")
            # part downloads
            parts_a = ((da or {}).get("MIME") or {}).get("Parts") or []
            parts_b = ((db or {}).get("MIME") or {}).get("Parts") or []
            if not parts_b and not parts_a:
                continue
            probs = []
            if len(parts_a) != len(parts_b):
                probs.append("parts %d vs %d" % (len(parts_a), len(parts_b)))
            for i in range(min(len(parts_a), len(parts_b))):
                pa = ours.get("/api/v1/messages/%s/mime/part/%d/download" % (qid(a["ID"]), i))
                pb = oracle.get("/api/v1/messages/%s/mime/part/%d/download" % (qid(b["ID"]), i))
                if pa.status != pb.status:
                    probs.append("part %d status %d vs %d" % (i, pa.status, pb.status))
                    continue
                if pa.body != pb.body:
                    probs.append("part %d bytes differ (len %d vs %d)" % (i, len(pa.body), len(pb.body)))
                exp_ct = pb.bare_ctype() if pb.bare_ctype() in ATTACH_ALLOWLIST else "application/octet-stream"
                if pa.bare_ctype() != exp_ct:
                    probs.append("part %d content-type %s, expected %s" % (i, pa.bare_ctype(), exp_ct))
                if not f.get("crlf"):
                    fa = cd_filename(pa.header("Content-Disposition"))
                    fb = cd_filename(pb.header("Content-Disposition"))
                    if fa is None or id_sub(fa, [a["ID"]]) != id_sub(fb or "", [b["ID"]]):
                        probs.append("part %d filename %r vs %r" % (i, fa and id_sub(fa, [a["ID"]]), fb and id_sub(fb, [b["ID"]])))
                inj = header_injection_problems(pa)
                if inj:
                    probs.append("part %d header problems %s" % (i, inj))
            if f["diff"] is True:
                check("AC-08", "DIFF [%s] /mime/part/{n}/download bytes, filename, allowlisted type" % n, not probs,
                      "; ".join(probs[:3]))
            else:
                info("AC-08", "DIFF [%s] part downloads (informational)" % n, "; ".join(probs[:3]) or "identical")
        # v2 list + v1 list equality
        with group("AC-04", "DIFF list projections"):
            ra, la = ours.getj("/api/v2/messages?limit=250")
            rb, lb = oracle.getj("/api/v2/messages?limit=250")
            diffs = []
            if subjects(la["items"]) != subjects(lb["items"]):
                diffs.append("order %s vs %s" % (subjects(la["items"])[:4], subjects(lb["items"])[:4]))
            else:
                for x, y in zip(la["items"], lb["items"]):
                    f = fmap.get(fixture_name(x))
                    if not f or f["diff"] is not True:
                        continue
                    d = first_diff(norm_msg(x, [x["ID"]], f["synth_msgid"]), norm_msg(y, [y["ID"]], f["synth_msgid"]))
                    if d:
                        diffs.append("%s: %s" % (f["name"], d))
            for k in ("total", "count", "start"):
                if la.get(k) != lb.get(k):
                    diffs.append("%s %s vs %s" % (k, la.get(k), lb.get(k)))
            check("AC-04", "DIFF /api/v2/messages items (newest first, normalized)", not diffs, "; ".join(diffs[:3]))
            ra, va = ours.getj("/api/v1/messages")
            rb, vb = oracle.getj("/api/v1/messages")
            diffs = []
            if subjects(va) != subjects(vb):
                diffs.append("order differs")
            else:
                for x, y in zip(va, vb):
                    f = fmap.get(fixture_name(x))
                    if f and f["diff"] is True:
                        d = first_diff(norm_msg(x, [x["ID"]], f["synth_msgid"]), norm_msg(y, [y["ID"]], f["synth_msgid"]))
                        if d:
                            diffs.append("%s: %s" % (f["name"], d))
            check("AC-06", "DIFF /api/v1/messages Message[] (normalized)", not diffs, "; ".join(diffs[:3]))
    else:
        skip("AC-04", "DIFF fixture detail/list JSON vs MailHog", nodiff)
        skip("AC-07", "DIFF /download vs MailHog", nodiff)
        skip("AC-08", "DIFF /mime/part/{n}/download vs MailHog", nodiff)

    # ---- AC-08 FIX on ours: injection + unsafe types via compat part download
    with group("AC-08", "part download safety (FIX)"):
        m = om.get("att-crlf")
        probs = []
        if m:
            for i in range(len(((m.get("MIME") or {}).get("Parts")) or [])):
                p = ours.get("/api/v1/messages/%s/mime/part/%d/download" % (qid(m["ID"]), i))
                probs += ["part %d: %s" % (i, x) for x in header_injection_problems(p)]
        check("AC-08", "CR/LF in part filename never reaches response headers", m is not None and not probs, "; ".join(probs[:3]))
        m = om.get("att-unsafe")
        bad = []
        if m:
            for i in range(len(((m.get("MIME") or {}).get("Parts")) or [])):
                p = ours.get("/api/v1/messages/%s/mime/part/%d/download" % (qid(m["ID"]), i))
                if p.status == 200 and p.bare_ctype() in ("text/html", "image/svg+xml"):
                    bad.append(i)
        check("AC-08", "text/html and image/svg+xml parts served as application/octet-stream", m is not None and not bad,
              "parts %s" % bad)

    # ---- AC-05 search
    with group("AC-05", "search"):
        searches = [("from", "ALICE@EXAMPLE"), ("from", "Bounce.Test"), ("to", "CAROL@rcpt"), ("to", "bob@EXAMPLE.test"),
                    ("containing", "=3D"), ("containing", "=?utf-8?"), ("containing", "こんにちは"),
                    ("containing", "x-fixture"), ("containing", "FIXTURE")]
        for kind, q in searches:
            path = "/api/v2/search?kind=%s&query=%s" % (kind, urllib.parse.quote(q))
            ra, ja = ours.getj(path)
            label = "search kind=%s (%d-char query)" % (kind, len(q))
            if ja is None:
                check("AC-05", label + " responds 200", False, "status=%d" % ra.status)
                continue
            if kind == "containing" and q in ("=3D", "こんにちは"):
                exp = [f["name"] for f in fixtures if q.lower().encode("utf-8") in f["data"].lower()]
                check("AC-05", label + " matches undecoded body/header values only (FIX)",
                      sorted(subjects(ja["items"])) == sorted(exp) and ja["total"] == len(exp),
                      "got %s expected %s" % (sorted(subjects(ja["items"])), sorted(exp)))
            if have:
                rb, jb = oracle.getj(path)
                d = None
                if jb is None:
                    d = "oracle status %d" % rb.status
                elif (ja["total"], ja["count"], ja["start"]) != (jb["total"], jb["count"], jb["start"]):
                    d = "total/count/start %s vs %s" % ((ja["total"], ja["count"], ja["start"]), (jb["total"], jb["count"], jb["start"]))
                elif subjects(ja["items"]) != subjects(jb["items"]):
                    d = "items %s vs %s" % (subjects(ja["items"])[:5], subjects(jb["items"])[:5])
                elif ra.bare_ctype() != rb.bare_ctype():
                    d = "content-type %s vs %s" % (ra.ctype(), rb.ctype())
                check("AC-05", "DIFF " + label, not d, d or "")
        for path in ["/api/v2/search?kind=containing&query=fixture&start=2&limit=3",
                     "/api/v2/search?kind=from&query=example&limit=0",
                     "/api/v2/search?kind=to&query=rcpt&start=abc&limit=2"]:
            ra, ja = ours.getj(path)
            if have:
                rb, jb = oracle.getj(path)
                same = ja is not None and jb is not None and (ja["total"], ja["count"], ja["start"], subjects(ja["items"])) == \
                    (jb["total"], jb["count"], jb["start"], subjects(jb["items"]))
                check("AC-05", "DIFF search paging %s" % path.split("?")[1], same,
                      "ours=%s oracle=%s" % (ja and (ja["total"], ja["count"], ja["start"]), jb and (jb["total"], jb["count"], jb["start"])))
            else:
                check("AC-05", "search paging %s responds 200 with {total,count,start,items}" % path.split("?")[1],
                      ja is not None and set(ja) == {"total", "count", "start", "items"})
        r = ours.get("/api/v2/search?kind=from&query=alice")
        check("AC-05", "search Content-Type application/json", r.bare_ctype() == "application/json", r.ctype())
        for path in ["/api/v2/search?kind=bogus&query=a", "/api/v2/search?kind=from&query=", "/api/v2/search?kind=from",
                     "/api/v2/search?query=a", "/api/v2/search"]:
            r = ours.get(path)
            exp = 400
            if have:
                exp = oracle.get(path).status
            check("AC-05", "invalid search %s -> %d" % (path.split("?")[1] if "?" in path else "(no params)", exp),
                  r.status == exp == 400, "ours=%d oracle/expected=%d" % (r.status, exp))

    # ---- AC-06 delete single / unknown ids
    with group("AC-06", "delete single"):
        m = om.get("plain")
        before = ours.getj("/api/v2/messages")[1]["total"]
        r = ours.req("DELETE", "/api/v1/messages/%s" % qid(m["ID"]))
        check("AC-06", "DELETE /api/v1/messages/{id} -> 200 with empty body", r.status == 200 and r.body == b"",
              "status=%d len=%d" % (r.status, len(r.body)))
        after2 = ours.getj("/api/v2/messages")[1]["total"]
        after1 = len(ours.getj("/api/v1/messages")[1])
        check("AC-06", "single delete reflected in v1 and v2", after2 == before - 1 and after1 == before - 1,
              "before=%d v2=%d v1=%d" % (before, after2, after1))
        check("AC-06", "deleted id -> 404", status_of(ours, "GET", "/api/v1/messages/%s" % qid(m["ID"])) == 404)
        if have:
            ob = oracle.getj("/api/v2/messages")[1]["total"]
            rr = oracle.req("DELETE", "/api/v1/messages/%s" % qid(tm["plain"]["ID"]))
            oa = oracle.getj("/api/v2/messages")[1]["total"]
            check("AC-06", "DIFF single DELETE status/body/total", (rr.status, rr.body, ob - oa) == (r.status, r.body, before - after2),
                  "oracle=%d/%d/%d" % (rr.status, len(rr.body), ob - oa))
        unknown = "no-such-id-" + secrets.token_hex(6)
        res = [status_of(ours, meth, p) for meth, p in [
            ("GET", "/api/v1/messages/%s" % unknown), ("DELETE", "/api/v1/messages/%s" % unknown),
            ("GET", "/api/v1/messages/%s/download" % unknown), ("GET", "/api/v1/messages/%s/mime/part/0/download" % unknown),
            ("GET", "/api/mailbox/messages/%s" % unknown)]]
        check("AC-06", "unknown id -> 404 on detail/delete/download/part/UI detail (documented divergence)",
              all(v == 404 for v in res), str(res))
        mp = om.get("mixed-pdf")
        if mp:
            st = status_of(ours, "GET", "/api/v1/messages/%s/mime/part/99/download" % qid(mp["ID"]))
            check("AC-08", "out-of-range part index -> 404 (MailHog drops the connection)", st == 404, "status=%d" % st)
        info("AC-06", "MailHog oracle: unknown id GET -> 200 'null', DELETE -> 500, download/part -> connection dropped (allowed divergence)")

    # ---- AC-26 envelope lifecycle
    with group("AC-26", "envelope lifecycle"):
        def lifecycle(ep):
            def pl(name):
                return build("From: Header Person <header@example.test>", "Subject: " + name, "X-Fixture: " + name, "", "lifecycle " + name)
            s = Smtp(ep.smtp)
            s.cmd("EHLO lifecycle-helo.test")
            c = [s.transaction("life-a@env.test", ["r1@rcpt.test", "r2@rcpt.test"], pl("life-1")),
                 s.transaction("life-b@env.test", ["r3@rcpt.test"], pl("life-2"), params="SIZE=123")]
            s.cmd("MAIL FROM:<life-x@env.test>")
            s.cmd("RCPT TO:<rx@rcpt.test>")
            s.cmd("RSET")
            c.append(s.transaction("life-c@env.test", ["r4@rcpt.test"], pl("life-3")))
            s.quit()
            s = Smtp(ep.smtp)
            s.cmd("EHLO " + "h" * 300)
            c.append(s.transaction("life-d@env.test", ["r5@rcpt.test"], pl("life-longhelo")))
            s.quit()
            return c, pl
        codes, pl = lifecycle(ours)
        check("AC-26", "two transactions per session, RSET, SIZE param and long HELO all accepted (250)", codes == [250] * 4, str(codes))
        s = Smtp(ours.smtp)
        s.cmd("EHLO null.test")
        cn = s.transaction("", ["r6@rcpt.test"], pl("life-null"))
        s.quit()
        check("AC-26", "null sender MAIL FROM:<> accepted", cn == 250, "code=%d" % cn)
        lm = v1_map(ours)
        exp = {"life-1": ("life-a@env.test", ["r1@rcpt.test", "r2@rcpt.test"]), "life-2": ("life-b@env.test", ["r3@rcpt.test"]),
               "life-3": ("life-c@env.test", ["r4@rcpt.test"]), "life-longhelo": ("life-d@env.test", ["r5@rcpt.test"])}
        probs = []
        for k, (fr, to) in exp.items():
            m = lm.get(k)
            if not m:
                probs.append("%s missing" % k)
                continue
            if m["Raw"]["From"] != fr or m["Raw"]["To"] != to or m["From"] != path_obj(fr):
                probs.append("%s envelope %r/%r" % (k, m["Raw"]["From"], m["Raw"]["To"]))
        check("AC-26", "per-transaction envelope (2nd tx, after RSET, SIZE param -> Params '')", not probs, "; ".join(probs))
        helos = {k: (lm.get(k) or {}).get("Raw", {}).get("Helo") for k in ("life-1", "life-2", "life-3")}
        check("AC-26", "HELO survives DATA and RSET within the session (ADR Amendment 1)",
              all(v == "lifecycle-helo.test" for v in helos.values()), str(helos))
        lh = (lm.get("life-longhelo") or {}).get("Raw", {}).get("Helo")
        check("AC-26", "HELO argument truncated to 255 chars", lh == "h" * 255, "len=%s" % (len(lh) if lh is not None else None))
        nm = lm.get("life-null")
        check("AC-26", "null sender -> Raw.From is empty string", bool(nm) and nm["Raw"]["From"] == "",
              "Raw.From=%r" % (nm and nm["Raw"]["From"]))
        if have:
            ocodes, _ = lifecycle(oracle)
            tl = v1_map(oracle)
            diffs = []
            for k in exp:
                a, b = lm.get(k), tl.get(k)
                if not a or not b:
                    diffs.append("%s missing" % k)
                    continue
                d = first_diff(norm_msg(a, [a["ID"]], True, drop_helo=(k != "life-1")),
                               norm_msg(b, [b["ID"]], True, drop_helo=(k != "life-1")))
                if d:
                    diffs.append("%s: %s" % (k, d))
            check("AC-26", "DIFF lifecycle messages vs MailHog (Helo/Received excluded after 1st tx)", not diffs, "; ".join(diffs[:3]))
            info("AC-26", "MailHog Helo for 2nd tx / after RSET / 300-char HELO",
                 "%r / %r / len %d (ours follows ADR: kept / kept / 255)" % (
                     (tl.get("life-2") or {}).get("Raw", {}).get("Helo"), (tl.get("life-3") or {}).get("Raw", {}).get("Helo"),
                     len((tl.get("life-longhelo") or {}).get("Raw", {}).get("Helo") or "")))
            s = Smtp(oracle.smtp)
            s.cmd("EHLO null.test")
            code, _ = s.cmd("MAIL FROM:<>")
            s.quit()
            info("AC-26", "MailHog rejects MAIL FROM:<>", "code=%d" % code)
        else:
            skip("AC-26", "DIFF lifecycle vs MailHog", nodiff)

    # ---- AC-03 paging + AC-06 v1 cap 1000
    with group("AC-03", "paging"):
        delete_all(ours)
        if have:
            delete_all(oracle)
        N = 260
        pls = [build("Subject: page-%04d" % i, "X-Fixture: page-%04d" % i, "", "paging body %d" % i) for i in range(N)]
        codes = send_bulk(ours, pls)
        check("AC-03", "%d paging messages accepted" % N, codes == [250] * N, "non-250: %d" % sum(1 for c in codes if c != 250))
        if have:
            send_bulk(oracle, pls)
        queries = ["", "?limit=0", "?limit=-1", "?limit=abc", "?limit=1", "?limit=250", "?limit=251", "?limit=999",
                   "?start=-1", "?start=abc", "?start=5&limit=3", "?start=258", "?start=300", "?limit=1.5",
                   "?limit=%2B3", "?limit=+3", "?limit=99999999999999999999", "?start=2&limit=0", "?start=&limit="]
        for q in queries:
            r, j = ours.getj("/api/v2/messages" + q)
            start, limit, count = expected_page(N, q)
            exp_items = ["page-%04d" % (N - 1 - start - k) for k in range(count)]
            ok = j is not None and (j["total"], j["count"], j["start"]) == (N, count, start) and subjects(j["items"]) == exp_items \
                and r.bare_ctype() == "text/json"
            check("AC-03", "GET /api/v2/messages%s -> total=%d count=%d start=%d newest-first" % (q or " (no params)", N, count, start), ok,
                  "got %s" % ((j and (j["total"], j["count"], j["start"], subjects(j["items"])[:2])),))
            if have:
                rb, jb = oracle.getj("/api/v2/messages" + q)
                same = j is not None and jb is not None and (j["total"], j["count"], j["start"], subjects(j["items"])) == \
                    (jb["total"], jb["count"], jb["start"], subjects(jb["items"]))
                check("AC-03", "DIFF GET /api/v2/messages%s" % (q or " (no params)"), same,
                      "oracle=%s" % ((jb and (jb["total"], jb["count"], jb["start"])),))
        if have:
            rb, jb = oracle.getj("/api/v2/messages?start=%d" % N)
            r, j = ours.getj("/api/v2/messages?start=%d" % N)
            info("AC-03", "start == total boundary (MailHog InMemory quirk, not compared)",
                 "ours count=%s oracle count=%s" % (j and j["count"], jb and jb["count"]))
        extra = [build("Subject: page-%04d" % i, "X-Fixture: page-%04d" % i, "", "paging body %d" % i) for i in range(N, 1005)]
        send_bulk(ours, extra)
        if have:
            send_bulk(oracle, extra)
        r, v = ours.getj("/api/v1/messages")
        ok = v is not None and len(v) == 1000 and fixture_name(v[0]) == "page-1004" and fixture_name(v[-1]) == "page-0005"
        check("AC-06", "GET /api/v1/messages caps at 1000, newest first (1005 stored)", ok,
              "len=%s first=%s last=%s" % (v and len(v), v and fixture_name(v[0]), v and fixture_name(v[-1])))
        if have:
            rb, vb = oracle.getj("/api/v1/messages")
            check("AC-06", "DIFF /api/v1/messages with 1005 stored", v is not None and vb is not None and subjects(v) == subjects(vb),
                  "oracle len=%s" % (vb and len(vb)))
        r = delete_all(ours)
        if have:
            delete_all(oracle)
        r, j = ours.getj("/api/v2/messages")
        check("AC-06", "DELETE all reflected (empty state again)", j == {"total": 0, "count": 0, "start": 0, "items": []})


# --------------------------------------------------------------------------
# UI API (decoded projection) — AC-10 / AC-11 / AC-12


def nl(s):
    return (s or "").replace("\r\n", "\n")


def ui_suite(ours, fixtures):
    delete_all(ours)
    codes = send_fixtures(ours, fixtures)
    check("AC-10", "fixtures re-sent for UI checks", all(c == 250 for c in codes.values()), str(codes))
    om = v1_map(ours)

    with group("AC-10", "UI list"):
        r, j = ours.getj("/api/mailbox/messages?start=0&limit=50")
        ok = j is not None and {"total", "start", "items"} <= set(j) and isinstance(j["items"], list)
        check("AC-10", "GET /api/mailbox/messages -> {total,start,items}", ok, "status=%d" % r.status)
        if ok:
            keys = {"id", "from", "to", "subject", "receivedAt", "size", "attachmentCount", "snippet"}
            bad = [it.get("id") for it in j["items"] if not keys <= set(it)]
            check("AC-10", "UI list items carry id/from/to/subject/receivedAt/size/attachmentCount/snippet", not bad,
                  "missing keys on %d items: %s" % (len(bad), sorted(keys - set(j["items"][0])) if j["items"] else ""))
            byid = {it.get("id"): it for it in j["items"]}
            probs = []
            for f in fixtures:
                m = om.get(f["name"])
                if not m or "subject" not in f:
                    continue
                it = byid.get(m["ID"])
                if not it:
                    probs.append("%s not listed by MailHog ID" % f["name"])
                elif it.get("subject") != f["subject"]:
                    probs.append("%s subject %r" % (f["name"], (it.get("subject") or "")[:40]))
            check("AC-10", "UI list subjects decoded (RFC2047 B/Q, ISO-2022-JP, Shift_JIS, EUC-JP, Latin-1)", not probs, "; ".join(probs[:4]))
            ids = [it.get("id") for it in j["items"]]
            exp_order = [om[f["name"]]["ID"] for f in reversed(fixtures) if f["name"] in om]
            check("AC-10", "UI list newest first", ids[:len(exp_order)] == exp_order)
            check("AC-10", "UI list never carries bodies (no text/html keys)",
                  all("text" not in it and "html" not in it for it in j["items"]))
        r, j = ours.getj("/api/mailbox/messages?limit=1000")
        check("AC-21", "UI list limit capped at 100 (ADR)", j is not None and len(j["items"]) <= 100, "status=%d" % r.status)
        r, j = ours.getj("/api/mailbox/messages?q=%s" % urllib.parse.quote("Plain ASCII"))
        check("AC-10", "UI list q= filters", j is not None and om.get("plain") is not None and
              [it.get("id") for it in j["items"]] == [om["plain"]["ID"]],
              "got %d items" % (len(j["items"]) if j else -1))

    detail_keys = {"id", "from", "to", "cc", "subject", "date", "receivedAt", "size", "headers", "text", "hasHtml", "attachments", "warnings"}
    for f in fixtures:
        n = f["name"]
        m = om.get(n)
        if not m:
            continue
        with group("AC-10", "[%s] UI detail" % n):
            r, d = ours.getj("/api/mailbox/messages/%s" % qid(m["ID"]))
            if not check("AC-10", "[%s] GET /api/mailbox/messages/{id} 200 with detail keys" % n,
                         d is not None and detail_keys <= set(d), "status=%d missing=%s" % (r.status, d and sorted(detail_keys - set(d)))):
                continue
            probs = []
            if "subject" in f and d["subject"] != f["subject"]:
                probs.append("subject")
            if "from_name" in f and (d["from"] or {}).get("name") != f["from_name"]:
                probs.append("from.name")
            if "from_addr" in f and (d["from"] or {}).get("address") != f["from_addr"]:
                probs.append("from.address")
            if not any(isinstance(x, dict) and x.get("address") == "bob@example.test" for x in (d["to"] or [])):
                probs.append("to[] lacks bob@example.test")
            text = nl(d.get("text"))
            for t in f.get("text", []):
                if nl(t) not in text:
                    probs.append("text lacks expected decoded string (len %d)" % len(t))
            for t in f.get("text_absent", []):
                if t in text:
                    probs.append("text contains preamble/epilogue")
            if f.get("warnings") is False and "\ufffd" in (text + (d["subject"] or "") + json.dumps(d["from"], ensure_ascii=False)):
                probs.append("U+FFFD present")
            if "has_html" in f and bool(d["hasHtml"]) != f["has_html"]:
                probs.append("hasHtml=%r" % d["hasHtml"])
            if not isinstance(d["headers"], list) or not all(isinstance(h, list) and len(h) == 2 for h in d["headers"]):
                probs.append("headers not [[name,value]]")
            check("AC-10", "[%s] decoded subject/from/to/text%s" % (n, " (strict Latin-1 0x80-0x9F)" if n == "latin1" else ""),
                  not probs, ", ".join(probs))
            if f.get("warnings") is True:
                check("AC-21", "[%s] warnings[] non-empty (failure identifiable)" % n, bool(d["warnings"]))
                rr = ours.get("/api/mailbox/messages/%s/raw" % qid(m["ID"]))
                check("AC-10", "[%s] raw source still retrievable" % n,
                      rr.status == 200 and ("X-Fixture: " + n).encode() in rr.body and rr.bare_ctype() == "text/plain",
                      "status=%d ctype=%s" % (rr.status, rr.ctype()))
            elif f.get("warnings") is False:
                check("AC-10", "[%s] no warnings for well-formed input" % n, d["warnings"] == [], "warnings=%d" % len(d["warnings"] or []))
            # attachments
            if "attachments" in f:
                atts = d["attachments"] or []
                exp = f["attachments"]
                ok = len(atts) == len(exp)
                probs = [] if ok else ["count %d vs %d" % (len(atts), len(exp))]
                for a, (fname, ctype, data) in zip(atts, exp):
                    if a.get("filename") != fname:
                        probs.append("filename %r" % a.get("filename"))
                    if (a.get("contentType") or "").split(";")[0].strip().lower() != ctype:
                        probs.append("contentType %r" % a.get("contentType"))
                    if a.get("size") != len(data):
                        probs.append("size %r vs %d" % (a.get("size"), len(data)))
                    dl = ours.get("/api/mailbox/messages/%s/attachments/%s" % (qid(m["ID"]), a.get("index")))
                    if dl.status != 200 or dl.body != data:
                        probs.append("download %s bytes differ (status %d, len %d vs %d)" % (fname, dl.status, len(dl.body), len(data)))
                    cd = dl.header("Content-Disposition") or ""
                    if not cd.lower().startswith("attachment"):
                        probs.append("Content-Disposition not attachment")
                    if "filename*=utf-8''" not in cd.lower() or cd_filename(cd) != fname:
                        probs.append("filename*=UTF-8'' missing or wrong")
                    if (dl.header("X-Content-Type-Options") or "").lower() != "nosniff":
                        probs.append("nosniff missing")
                    exp_ct = ctype if ctype in ATTACH_ALLOWLIST else "application/octet-stream"
                    if dl.bare_ctype() != exp_ct:
                        probs.append("served as %s, expected %s" % (dl.bare_ctype(), exp_ct))
                    probs += header_injection_problems(dl)
                check("AC-11", "[%s] attachments listed (decoded filename/type/size) and downloaded byte-exact, safe headers" % n,
                      not probs, "; ".join(probs[:4]))
            if f.get("crlf"):
                atts = d["attachments"] or []
                probs = []
                for a in atts:
                    if "\r" in (a.get("filename") or "") or "\n" in (a.get("filename") or ""):
                        info("AC-11", "[%s] decoded filename in JSON keeps CR/LF (JSON-escaped)" % n)
                    dl = ours.get("/api/mailbox/messages/%s/attachments/%s" % (qid(m["ID"]), a.get("index")))
                    probs += header_injection_problems(dl)
                    if not (dl.header("Content-Disposition") or "").lower().startswith("attachment"):
                        probs.append("not attachment")
                check("AC-11", "[%s] CR/LF in decoded filename never reaches response headers" % n,
                      bool(atts) and not probs, "; ".join(probs[:3]) or "no attachments listed")
            # HTML endpoint
            if f.get("has_html"):
                h = ours.get("/api/mailbox/messages/%s/html" % qid(m["ID"]))
                csp = h.header("Content-Security-Policy") or ""
                probs = []
                if h.status != 200 or h.bare_ctype() != "text/html":
                    probs.append("status %d ctype %s" % (h.status, h.ctype()))
                if "sandbox allow-popups allow-popups-to-escape-sandbox" not in csp:
                    probs.append("CSP lacks 'sandbox allow-popups allow-popups-to-escape-sandbox'")
                if "allow-scripts" in csp or "allow-same-origin" in csp:
                    probs.append("CSP allows scripts/same-origin")
                for directive in ("default-src 'none'", "frame-ancestors 'self'", "form-action 'none'"):
                    if directive not in csp:
                        probs.append("CSP lacks %s" % directive)
                if (h.header("Referrer-Policy") or "").lower() != "no-referrer":
                    probs.append("Referrer-Policy %r" % h.header("Referrer-Policy"))
                if (h.header("X-Content-Type-Options") or "").lower() != "nosniff":
                    probs.append("nosniff missing")
                if not re.search(rb"<base\s+target=[\"']_blank[\"']\s*/?>", h.body, re.I):
                    probs.append('<base target="_blank"> not injected')
                for s in f.get("html_contains", []):
                    if s.encode() not in h.body:
                        probs.append("link missing")
                check("AC-12a", "[%s] HTML endpoint: CSP sandbox allow-popups*, no-referrer, nosniff, <base target=_blank>" % n,
                      not probs, "; ".join(probs))
        # raw endpoint (every fixture)
    with group("AC-10", "raw endpoint"):
        m = om.get("plain")
        rr = ours.get("/api/mailbox/messages/%s/raw" % qid(m["ID"]))
        check("AC-10", "GET .../raw -> text/plain; charset=utf-8 + nosniff, contains source",
              rr.status == 200 and rr.ctype().replace(" ", "").lower() == "text/plain;charset=utf-8" and
              (rr.header("X-Content-Type-Options") or "").lower() == "nosniff" and
              b"X-Fixture: plain" in rr.body and b"Hello plain world." in rr.body,
              "status=%d ctype=%s" % (rr.status, rr.ctype()))
    return om


# --------------------------------------------------------------------------
# SSE (AC-09) on main


def sse_suite(ours):
    with group("AC-09", "SSE live delivery"):
        c1, c2 = SSEClient(ours), SSEClient(ours)
        check("AC-09", "GET /api/v1/events -> 200 text/event-stream, Cache-Control no-cache",
              c1.status == 200 and c1.resp.bare_ctype() == "text/event-stream" and
              "no-cache" in (c1.resp.header("Cache-Control") or "").lower(),
              "status=%d ctype=%s cc=%s" % (c1.status, c1.resp.ctype(), c1.resp.header("Cache-Control")))
        time.sleep(0.3)
        name = "sse-live-" + secrets.token_hex(3)
        code = send_mail(ours, build("Subject: " + name, "X-Fixture: " + name, "", "sse body"))
        t_sent = time.monotonic()
        e1, e2 = c1.event_for(name, 3.0), c2.event_for(name, 3.0)
        lat = [round(e[0] - t_sent, 3) if e else None for e in (e1, e2)]
        check("AC-09", "two concurrent SSE clients both receive the new message within 1s of SMTP 250",
              code == 250 and all(x is not None and x <= 1.0 for x in lat), "latency=%s" % lat)
        if e1:
            r, d = ours.getj("/api/v1/messages/%s" % qid(e1[1]["ID"]))
            check("AC-09", "data: payload equals GET /api/v1/messages/{id}", d == e1[1], first_diff(e1[1], d) or "")
        if SLOW:
            got = c1.wait(lambda c: c.comments, 31.0)
            check("AC-09", "SSE comment heartbeat within 30s [SLOW]", bool(got))
        else:
            skip("AC-09", "SSE heartbeat within 30s [SLOW]", "set E2E_SLOW=1")
        c1.close()
        c2.close()


# --------------------------------------------------------------------------
# hostile input (AC-21) and limits (AC-22)


def hostile_suite(ours, args):
    with group("AC-21", "hostile MIME"):
        sse = SSEClient(ours)
        hostile = {}
        depth = 10000
        parts = [build("Subject: hostile-deep", "X-Fixture: hostile-deep")]
        for i in range(depth):
            parts.append(build('Content-Type: multipart/mixed; boundary="d%05d"' % i, "", "--d%05d" % i))
        parts.append(build("Content-Type: text/plain", "", "deepest"))
        for i in reversed(range(depth)):
            parts.append(("--d%05d--" % i).encode())
        hostile["hostile-deep"] = CRLF.join(parts)
        hostile["hostile-100k-parts"] = build("Subject: hostile-100k-parts", "X-Fixture: hostile-100k-parts",
                                              'Content-Type: multipart/mixed; boundary="p"', "") + \
            CRLF + b"\r\n".join([b"--p\r\n\r\nx"] * 100000) + b"\r\n--p--"
        hostile["hostile-2mb-header"] = build("Subject: hostile-2mb-header", "X-Fixture: hostile-2mb-header",
                                              "X-Big: start", *([" " + "a" * 998] * 2100), "Content-Type: text/plain", "", "after big header")
        hostile["hostile-20k-fields"] = build("Subject: hostile-20k-fields", "X-Fixture: hostile-20k-fields",
                                              *(["X-H%d: v" % i for i in range(20000)]), "", "after many fields")
        codes = {}
        for k, v in hostile.items():
            try:
                codes[k] = send_mail(ours, v, timeout=120.0)
            except (OSError, EOFError, ValueError) as e:
                codes[k] = type(e).__name__
        check("AC-21", "SMTP answers 250 for 10k-deep nesting, 100k parts, 2MB header, 20k header fields", all(c == 250 for c in codes.values()), str(codes))
        alive = ours.get("/api/v2/messages?limit=1")
        check("AC-21", "process alive after hostile input", alive.status == 200, "status=%d" % alive.status)
        hm = v1_map(ours)
        check("AC-21", "GET /api/v1/messages lists hostile messages", all(k in hm for k in hostile), "missing %s" % [k for k in hostile if k not in hm])
        for path in ["/api/v2/messages?limit=250", "/api/v2/search?kind=containing&query=hostile", "/api/mailbox/messages?limit=100"]:
            r = ours.get(path, timeout=60)
            check("AC-21", "%s responds 200 with hostile messages stored" % path.split("?")[0], r.status == 200, "status=%d" % r.status)
        for k in hostile:
            m = hm.get(k)
            if not m:
                continue
            r1 = ours.get("/api/v1/messages/%s" % qid(m["ID"]), timeout=60)
            r2, d = ours.getj("/api/mailbox/messages/%s" % qid(m["ID"]), timeout=60)
            check("AC-21", "[%s] compat detail + UI detail respond, UI warnings[] non-empty" % k,
                  r1.status == 200 and d is not None and bool(d.get("warnings")),
                  "compat=%d ui=%d warnings=%s" % (r1.status, r2.status, d and len(d.get("warnings") or [])))
        sentinel = "sentinel-" + secrets.token_hex(3)
        send_mail(ours, build("Subject: " + sentinel, "X-Fixture: " + sentinel, "", "sentinel"))
        check("AC-21", "SSE stream still delivers after hostile messages", bool(sse.event_for(sentinel, 5.0)))
        sse.close()

    with group("AC-21", "random MIME corpus"):
        delete_all(ours)
        rnd = random.Random(20261004)
        corpus = [random_mail(rnd, i) for i in range(1000)]
        codes = send_bulk(ours, corpus, per_session=100, helo="random.test")
        check("AC-21", "1000 fixed-seed random MIME messages accepted (250)", all(c == 250 for c in codes),
              "non-250: %d" % sum(1 for c in codes if c != 250))
        r, v = ours.getj("/api/v1/messages", timeout=120)
        bad = []
        for m in v or []:
            a = ours.get("/api/mailbox/messages/%s" % qid(m["ID"]))
            if a.status != 200:
                bad.append(m["ID"])
        check("AC-21", "UI detail answers 200 for every random message (no parse panic)", v is not None and len(v) == 1000 and not bad,
              "listed=%s failed=%d" % (v and len(v), len(bad)))
        check("AC-21", "process alive after random corpus", ours.get("/api/v2/messages?limit=1").status == 200)
        delete_all(ours)

    with group("AC-22", "SMTP 50MB line"):
        rss0 = rss_kb(args.ours_pid)
        for phase in ("command", "data"):
            res = smtp_flood(ours, phase)
            check("AC-22", "50MB line without LF in %s phase -> error/close before all bytes read" % phase, res["closed"],
                  "sent=%dMB reply=%s" % (res["sent"] >> 20, res["reply"]))
        rss1 = rss_kb(args.ours_pid)
        if rss0 is not None and rss1 is not None:
            check("AC-22", "RSS growth after 2x50MB lines < 64MB", rss1 - rss0 < 65536, "rss %dKB -> %dKB" % (rss0, rss1))
        else:
            skip("AC-22", "RSS growth check", "no --ours-pid")
        s = Smtp(ours.smtp)
        check("AC-22", "SMTP still serves new sessions", s.banner[0] == 220)
        s.quit()
        check("AC-22", "HTTP still serves", ours.get("/api/v2/messages?limit=1").status == 200)

    with group("AC-22", "HTTP limits"):
        r = None
        try:
            r = ours.get("/api/v2/messages", headers=[("X-Huge", "a" * (100 * 1024))])
        except (OSError, EOFError, ValueError):
            pass
        check("AC-22", "request with 100KiB header rejected (4xx) or closed (64KiB cap, ADR)", r is None or 400 <= r.status < 500,
              "status=%s" % (r and r.status))
        if SLOW:
            for label, payload in (("idle", b""), ("partial request line", b"GET /api/v2/messages HTTP/1.1\r\nHost: 127.0.0.1\r\n")):
                t, closed = http_idle_close(ours, payload, 20.0)
                check("AC-22", "HTTP header read timeout closes %s connection in ~10s [SLOW]" % label, closed and 8.0 <= t <= 14.0,
                      "closed=%s after %.1fs" % (closed, t))
        else:
            skip("AC-22", "HTTP header read timeout 10s [SLOW]", "set E2E_SLOW=1")


def random_mail(rnd, i):
    charsets = ["utf-8", "iso-8859-1", "shift_jis", "euc-jp", "iso-2022-jp", "x-bogus", "", "utf-7", "windows-1252", "replacement"]
    ctypes = ["text/plain", "text/html", "multipart/mixed", "multipart/alternative", "multipart/related",
              "application/octet-stream", "message/rfc822", "image/png", "multipart/digest", "text/"]
    ctes = ["", "base64", "quoted-printable", "7bit", "8bit", "binary", "x-uuencode", "BASE64 "]

    def junk_line():
        k = rnd.randrange(8)
        if k == 0:
            return bytes(rnd.choice([b for b in range(1, 256) if b not in (10, 13)]) for _ in range(rnd.randrange(0, 200)))
        if k == 1:
            return b"=?" + rnd.choice([b"utf-8", b"bogus", b"iso-2022-jp", b""]) + b"?" + rnd.choice([b"B", b"Q", b"X"]) + b"?" + \
                base64.b64encode(bytes(rnd.randrange(256) for _ in range(rnd.randrange(0, 30)))) + rnd.choice([b"?=", b""])
        if k == 2:
            return b"=" + bytes(rnd.choice(b"0123456789ABCDEFZ=") for _ in range(rnd.randrange(0, 4)))
        if k == 3:
            return b"x" * rnd.randrange(0, 3000)
        return bytes(rnd.choice(b"abcdefghijklmnopqrstuvwxyz0123456789+/= -;:\"'<>") for _ in range(rnd.randrange(0, 120)))

    def entity(depth):
        lines = []
        b = rnd.choice(["b%d" % depth, "", "=_x", "-", "a b", "%d" % rnd.randrange(10 ** 6)])
        ct = rnd.choice(ctypes)
        params = ""
        if ct.startswith("multipart") and rnd.random() < 0.85:
            params += '; boundary="%s"' % b
        if rnd.random() < 0.5:
            params += "; charset=%s" % rnd.choice(charsets)
        if rnd.random() < 0.3:
            params += "; name*=%s''%s" % (rnd.choice(["UTF-8", "bogus", ""]), rnd.choice(["%E8%AB%8B", "%ZZ", "a%0D%0Ab", "plain"]))
        lines.append(("Content-Type: " + ct + params).encode())
        if rnd.random() < 0.6:
            lines.append(("Content-Transfer-Encoding: " + rnd.choice(ctes)).encode())
        if rnd.random() < 0.3:
            lines.append(b"Content-Disposition: " + rnd.choice([b"attachment", b"inline", b"attachment; filename=", b"x; filename*=utf-8''%FF"]))
        lines.append(b"")
        if ct.startswith("multipart") and depth < 6:
            if rnd.random() < 0.3:
                lines.append(junk_line())
            for _ in range(rnd.randrange(0, 5)):
                lines.append(("--" + b).encode())
                lines.extend(entity(depth + 1))
            if rnd.random() < 0.7:
                lines.append(("--" + b + "--").encode())
            if rnd.random() < 0.3:
                lines.append(junk_line())
        else:
            for _ in range(rnd.randrange(0, 12)):
                lines.append(junk_line())
        return lines

    head = [("Subject: random-%04d" % i).encode(), ("X-Fixture: random-%04d" % i).encode()]
    if rnd.random() < 0.3:
        head.append(b"Subject2: " + junk_line())
    return CRLF.join(head + entity(0))


def smtp_flood(ep, phase, total=50 << 20):
    res = {"closed": False, "sent": 0, "reply": None}
    try:
        s = Smtp(ep.smtp, timeout=30)
        s.cmd("EHLO flood.test")
        if phase == "data":
            s.cmd("MAIL FROM:<flood@bounce.test>")
            s.cmd("RCPT TO:<flood@rcpt.test>")
            code, _ = s.cmd("DATA")
            if code != 354:
                res["reply"] = code
                s.sock.close()
                return res
    except (OSError, EOFError) as e:
        res["reply"] = "setup failed: %s" % type(e).__name__
        return res
    try:
        chunk = b"A" * (1 << 20)
        try:
            while res["sent"] < total:
                s.sock.sendall(chunk)
                res["sent"] += len(chunk)
        except socket.timeout:
            pass  # server stopped reading without closing: decided by the recv below
        except OSError:
            res["closed"] = True  # EPIPE / ECONNRESET: server closed while we were sending
        # the server must answer with an error and close the connection (AC-22)
        s.sock.settimeout(15)
        try:
            data = s.sock.recv(4096)
            while data:
                m = re.search(rb"(?m)^([45]\d\d)", data)
                if m:
                    res["reply"] = int(m.group(1))
                data = s.sock.recv(4096)
            res["closed"] = True
        except socket.timeout:
            pass
        except OSError:
            res["closed"] = True
    finally:
        try:
            s.sock.close()
        except OSError:
            pass
    return res


def http_idle_close(ep, payload, limit):
    s = socket.create_connection(("127.0.0.1", ep.http), timeout=limit)
    t0 = time.monotonic()
    try:
        if payload:
            s.sendall(payload)
        try:
            while s.recv(4096):
                pass
            return time.monotonic() - t0, True
        except ConnectionResetError:
            return time.monotonic() - t0, True
        except socket.timeout:
            return time.monotonic() - t0, False
    finally:
        s.close()


def rss_kb(pid):
    if not pid:
        return None
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True, timeout=10).stdout.strip()
        return int(out) if out else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


class IdleSmtpWatcher(threading.Thread):
    """AC-22 SMTP idle timeout (300 s) observed in the background [SLOW]."""

    def __init__(self, ep):
        super().__init__(daemon=True)
        self.ep = ep
        self.result = None

    def run(self):
        try:
            s = Smtp(self.ep.smtp, timeout=360)
            t0 = time.monotonic()
            try:
                while s.sock.recv(4096):
                    pass
                self.result = time.monotonic() - t0
            except ConnectionResetError:
                self.result = time.monotonic() - t0
            except socket.timeout:
                self.result = -1
        except OSError:
            self.result = -2


# --------------------------------------------------------------------------
# HTTP surface hygiene: AC-15, AC-23, AC-27


def surface_suite(ours):
    with group("AC-15", "traversal / CORS"):
        for p in ["/%2e%2e/%2e%2e/etc/passwd", "/assets/../../Cargo.toml", "/assets/..%2f..%2fCargo.toml",
                  "/assets/%2e%2e/%2e%2e/Cargo.toml", "/assets/..\\..\\Cargo.toml", "/assets/%00x.js", "/assets//x.js",
                  "/assets/does-not-exist.js", "/api/does-not-exist"]:
            r = ours.get(p)
            check("AC-15", "%s -> 404" % p, r.status == 404 and b"[workspace]" not in r.body and b"root:" not in r.body,
                  "status=%d" % r.status)
        r = ours.get("/api/v2/messages", headers=[("Origin", "https://evil.example")])
        check("AC-15", "no Access-Control-Allow-Origin with Origin header", not r.has("Access-Control-Allow-Origin"))
        r = ours.req("OPTIONS", "/api/v2/messages", headers=[("Origin", "https://evil.example"),
                                                             ("Access-Control-Request-Method", "DELETE")])
        check("AC-15", "CORS preflight gets no Access-Control-* headers, OPTIONS -> 405 (ADR)",
              r.status == 405 and not any(k.lower().startswith("access-control-") for k, _ in r.headers), "status=%d" % r.status)
        r = ours.req("POST", "/api/v1/messages", body=b"x")
        check("AC-15", "POST on API does not mutate (non-2xx)", not (200 <= r.status < 300), "status=%d" % r.status)

    with group("AC-23", "Host allowlist"):
        port = ours.http
        for h in ["evil.example", "evil.example:%d" % port, "localhost.evil.example", "127.0.0.1.nip.io"]:
            r = ours.get("/api/v2/messages", host=h)
            check("AC-23", "Host: %s -> 403" % h, r.status == 403 and b"DEVCLOUD_MAILBOX_ALLOWED_HOSTS" in r.body,
                  "status=%d names-env-var=%s" % (r.status, b"DEVCLOUD_MAILBOX_ALLOWED_HOSTS" in r.body))
        r = ours.get("/", host="evil.example")
        check("AC-23", "Host check also guards the SPA", r.status == 403, "status=%d" % r.status)
        for h in ["localhost", "localhost:%d" % port, "127.0.0.1", "127.0.0.1:%d" % port, "mailbox", "mailbox:8025",
                  "host.docker.internal", "host.docker.internal:8025", "app.localhost", "[::1]:%d" % port, "10.1.2.3:8025"]:
            r = ours.get("/api/v2/messages", host=h)
            check("AC-23", "Host: %s allowed" % h, r.status == 200, "status=%d" % r.status)

    with group("AC-30", "actionable Host-denied 403 body"):
        r = ours.get("/api/v2/messages", host="evil.example")
        body = r.body.decode("utf-8", "replace")
        check("AC-30", "Host: evil.example -> 403 text/plain naming the host, DEVCLOUD_MAILBOX_ALLOWED_HOSTS and '*'",
              r.status == 403 and r.bare_ctype() == "text/plain" and "evil.example" in body and
              "DEVCLOUD_MAILBOX_ALLOWED_HOSTS" in body and "*" in body,
              "status=%d ctype=%s host=%s env=%s star=%s" % (r.status, r.ctype(), "evil.example" in body,
                                                             "DEVCLOUD_MAILBOX_ALLOWED_HOSTS" in body, "*" in body))
        csp = r.header("Content-Security-Policy") or ""
        check("AC-30", "403 keeps nosniff and the /api/* CSP sandbox",
              (r.header("X-Content-Type-Options") or "").lower() == "nosniff" and "sandbox" in csp and "default-src 'none'" in csp,
              "nosniff=%r csp=%r" % (r.header("X-Content-Type-Options"), csp[:60]))
        r = ours.get("/", host="evil.example")
        check("AC-30", "SPA 403 is text/plain with the same guidance and nosniff",
              r.status == 403 and r.bare_ctype() == "text/plain" and b"DEVCLOUD_MAILBOX_ALLOWED_HOSTS" in r.body and
              (r.header("X-Content-Type-Options") or "").lower() == "nosniff", "status=%d ctype=%s" % (r.status, r.ctype()))
        r = ours.get("/api/v2/messages", host="evil\x01\x1b[31m\x7f.example")
        ctl = [b for b in r.body if (b < 0x20 and b not in (0x09, 0x0a, 0x0d)) or b == 0x7f]
        check("AC-30", "control characters in Host: 403 with the host stripped in the body, or 400 at parsing; no control bytes",
              not ctl and ((r.status == 403 and b"evil" in r.body and b".example" in r.body) or r.status == 400),
              "status=%d ctl-bytes=%d" % (r.status, len(ctl)))
        long_host = "x" * 292 + ".example"
        r = ours.get("/api/v2/messages", host=long_host)
        check("AC-30", "300-char denied Host is echoed capped at 255 chars",
              r.status == 403 and long_host[:255].encode() in r.body and long_host[:256].encode() not in r.body,
              "status=%d has-255=%s has-256=%s" % (r.status, long_host[:255].encode() in r.body, long_host[:256].encode() in r.body))

    with group("AC-27", "embedded assets"):
        r = ours.get("/")
        check("AC-27", "GET / serves embedded index.html", r.status == 200 and r.bare_ctype() == "text/html", "status=%d" % r.status)
        refs = sorted(set(re.findall(rb"""(?:src|href)=["'](?:\./|/)?(assets/[^"'?#]+)""", r.body)))
        check("AC-27", "index.html references /assets/*", bool(refs), "none found")
        missing = []
        for ref in refs:
            a = ours.get("/" + ref.decode())
            if a.status != 200 or not a.body:
                missing.append(ref.decode())
            elif ref.endswith(b".js") and "javascript" not in a.bare_ctype():
                missing.append("%s ctype %s" % (ref.decode(), a.ctype()))
            elif ref.endswith(b".css") and a.bare_ctype() != "text/css":
                missing.append("%s ctype %s" % (ref.decode(), a.ctype()))
        check("AC-27", "every asset referenced by index.html is embedded and served (%d refs)" % len(refs), not missing, str(missing[:3]))
        r = ours.get("/some/client/route")
        check("AC-27", "extensionless unknown path falls back to index.html", r.status == 200 and r.bare_ctype() == "text/html")
        return refs


# --------------------------------------------------------------------------
# auxiliary instances (strict mode, hosts env, restart, startup failures, SSE cap)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


AUX_LOGS = []
LINE1 = re.compile(r"(?i)(line\D{0,3}1\b|jsonl:1\b)")


class Instance:
    def __init__(self, binpath, workdir, label, env=None, storage=None):
        self.label = label
        self.smtp = free_port()
        self.http = free_port()
        self.storage = storage or tempfile.mkdtemp(prefix="aux-%s-" % label, dir=workdir)
        self.logpath = os.path.join(workdir, "aux-%s-%s.log" % (label, secrets.token_hex(3)))
        AUX_LOGS.append(self.logpath)
        e = {k: v for k, v in os.environ.items() if not k.startswith("DEVCLOUD_MAILBOX_")}
        e.update({"DEVCLOUD_MAILBOX_SMTP_ADDR": "127.0.0.1:%d" % self.smtp,
                  "DEVCLOUD_MAILBOX_HTTP_ADDR": "127.0.0.1:%d" % self.http,
                  "DEVCLOUD_MAILBOX_STORAGE": self.storage})
        e.update(env or {})
        self.log = open(self.logpath, "wb")
        self.proc = subprocess.Popen([binpath], env=e, stdout=self.log, stderr=subprocess.STDOUT)

    def ready(self, timeout=20.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return False
            ok = 0
            for p in (self.smtp, self.http):
                try:
                    socket.create_connection(("127.0.0.1", p), timeout=0.5).close()
                    ok += 1
                except OSError:
                    pass
            if ok == 2:
                return True
            time.sleep(0.1)
        return False

    def exit_within(self, timeout):
        try:
            return self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            return None

    def stop(self, timeout=15.0):
        t0 = time.monotonic()
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        code = self.exit_within(timeout)
        elapsed = time.monotonic() - t0
        if code is None:
            self.proc.kill()
            self.proc.wait()
        self.log.close()
        return code, elapsed

    def output(self):
        with open(self.logpath, "rb") as fh:
            return fh.read().decode("utf-8", "replace")

    def ep(self, creds=None):
        return Endpoint("aux-" + self.label, self.http, self.smtp, True, creds)


def aux_suite(args, secrets_seen):
    binpath, work = args.bin, args.workdir

    # ---- AC-14 strict mode (+ AC-23 host check before auth)
    with group("AC-14", "strict mode"):
        user, pw = "e2e-user", "pw-" + secrets.token_urlsafe(18)
        secrets_seen += [pw, base64.b64encode(("%s:%s" % (user, pw)).encode()).decode()]
        inst = Instance(binpath, work, "strict", {"DEVCLOUD_MAILBOX_AUTH_MODE": "strict",
                                                  "DEVCLOUD_MAILBOX_USERNAME": user, "DEVCLOUD_MAILBOX_PASSWORD": pw})
        try:
            if check("AC-14", "strict instance starts", inst.ready()):
                good = inst.ep((user, pw))
                anon = inst.ep()
                wrong = inst.ep((user, pw + "x"))
                send_mail(good, build("Subject: strict", "X-Fixture: strict", "", "strict body"))
                _, lst = good.getj("/api/v1/messages")
                mid = qid(lst[0]["ID"]) if lst else "x"
                idx = good.get("/")
                asset = re.search(rb"""(?:src|href)=["'](?:\./|/)?(assets/[^"'?#]+)""", idx.body)
                routes = ["/", "/some/spa/route", "/" + (asset.group(1).decode() if asset else "assets/missing.js"),
                          "/api/v1/messages", "/api/v2/messages", "/api/v2/search?kind=from&query=a", "/api/v1/messages/" + mid,
                          "/api/v1/messages/%s/download" % mid, "/api/mailbox/messages", "/api/mailbox/messages/" + mid,
                          "/api/mailbox/messages/%s/raw" % mid]
                for p in routes:
                    a, w, g = anon.get(p), wrong.get(p), good.get(p)
                    check("AC-14", "strict %s: no creds 401+WWW-Authenticate, wrong creds 401, correct Basic 200" % p.split("?")[0],
                          a.status == 401 and (a.header("WWW-Authenticate") or "").lower().startswith("basic") and w.status == 401 and g.status == 200,
                          "anon=%d(%s) wrong=%d good=%d" % (a.status, bool(a.header("WWW-Authenticate")), w.status, g.status))
                a = SSEClient(anon, read=False)
                g = SSEClient(good, read=False)
                check("AC-14", "strict /api/v1/events: no creds 401, correct Basic 200", a.status == 401 and g.status == 200,
                      "anon=%d good=%d" % (a.status, g.status))
                a.close()
                g.close()
                r = anon.req("DELETE", "/api/v1/messages")
                after = good.getj("/api/v1/messages")[1]
                check("AC-14", "strict DELETE without creds -> 401 and nothing deleted", r.status == 401 and bool(after), "status=%d" % r.status)
                r = anon.get("/api/v2/messages", host="evil.example")
                check("AC-23", "Host allowlist runs before auth (evil host -> 403 even without creds)", r.status == 403, "status=%d" % r.status)
                # AC-18: authenticated traffic must not leak creds into logs (checked at the end)
        finally:
            inst.stop()
        inst = Instance(binpath, work, "strict-nocreds", {"DEVCLOUD_MAILBOX_AUTH_MODE": "strict"})
        code = inst.exit_within(10)
        inst.stop()
        check("AC-14", "strict without username/password is a startup error", code is not None and code != 0, "exit=%s" % code)

    # ---- AC-23 allowlist env
    with group("AC-23", "DEVCLOUD_MAILBOX_ALLOWED_HOSTS"):
        inst = Instance(binpath, work, "hosts", {"DEVCLOUD_MAILBOX_ALLOWED_HOSTS": "evil.example,Other.Example"})
        try:
            if check("AC-23", "instance with ALLOWED_HOSTS starts", inst.ready()):
                e = inst.ep()
                check("AC-23", "ALLOWED_HOSTS adds hosts", e.get("/api/v2/messages", host="evil.example").status == 200 and
                      e.get("/api/v2/messages", host="other.example:8025").status == 200)
                check("AC-23", "hosts outside ALLOWED_HOSTS still 403", e.get("/api/v2/messages", host="third.example").status == 403)
                check("AC-23", "built-in hosts still allowed with ALLOWED_HOSTS set", e.get("/api/v2/messages", host="localhost").status == 200)
        finally:
            inst.stop()
        inst = Instance(binpath, work, "hosts-any", {"DEVCLOUD_MAILBOX_ALLOWED_HOSTS": "*"})
        try:
            if check("AC-23", "instance with ALLOWED_HOSTS=* starts", inst.ready()):
                check("AC-23", "ALLOWED_HOSTS=* disables the check", inst.ep().get("/api/v2/messages", host="anything.example").status == 200)
        finally:
            inst.stop()

    # ---- AC-29 ephemeral mode (persistence with the variable unset is AC-02 below; "false" checked here too)
    with group("AC-29", "ephemeral mode"):
        for value in ("true", "1", "YES", "True"):
            inst = Instance(binpath, work, "ephemeral", {"DEVCLOUD_MAILBOX_EPHEMERAL": value})
            storage = inst.storage
            try:
                if not check("AC-29", "EPHEMERAL=%s instance starts" % value, inst.ready()):
                    continue
                e = inst.ep()
                code = send_mail(e, build("Subject: ephemeral", "X-Fixture: ephemeral", "", "ephemeral body"))
                _, j = e.getj("/api/v2/messages")
                check("AC-29", "EPHEMERAL=%s: mail is listed while running" % value, code == 250 and j is not None and j["total"] == 1,
                      "smtp=%s total=%s" % (code, j and j["total"]))
                left = os.listdir(storage)
                check("AC-29", "EPHEMERAL=%s: nothing written under DEVCLOUD_MAILBOX_STORAGE while running" % value, not left,
                      "entries: %s" % left[:5])
            finally:
                exit_code, _ = inst.stop()
            out = inst.output()
            check("AC-29", "EPHEMERAL=%s: startup log says ephemeral, SIGTERM exits 0" % value,
                  re.search(r"ephemeral", out, re.I) is not None and exit_code == 0, "exit=%s" % exit_code)
            inst = Instance(binpath, work, "ephemeral2", {"DEVCLOUD_MAILBOX_EPHEMERAL": value}, storage=storage)
            try:
                if check("AC-29", "EPHEMERAL=%s restart on same storage" % value, inst.ready()):
                    _, j = inst.ep().getj("/api/v2/messages")
                    check("AC-29", "EPHEMERAL=%s: inbox empty after restart" % value, j == {"total": 0, "count": 0, "start": 0, "items": []},
                          "total=%s" % (j and j["total"]))
            finally:
                inst.stop()
            check("AC-29", "EPHEMERAL=%s: storage dir still empty after shutdown" % value, not os.listdir(storage),
                  "entries: %s" % os.listdir(storage)[:5])
        inst = Instance(binpath, work, "ephemeral-false", {"DEVCLOUD_MAILBOX_EPHEMERAL": "false"})
        storage = inst.storage
        try:
            if check("AC-29", "EPHEMERAL=false instance starts", inst.ready()):
                send_mail(inst.ep(), build("Subject: durable", "X-Fixture: durable", "", "durable body"))
        finally:
            inst.stop()
        inst = Instance(binpath, work, "ephemeral-false2", {"DEVCLOUD_MAILBOX_EPHEMERAL": "false"}, storage=storage)
        try:
            if check("AC-29", "EPHEMERAL=false restart on same storage", inst.ready()):
                _, j = inst.ep().getj("/api/v2/messages")
                check("AC-29", "EPHEMERAL=false keeps persistence across restart (unset: see AC-02)", j is not None and j["total"] == 1,
                      "total=%s" % (j and j["total"]))
        finally:
            inst.stop()

    # ---- AC-02 / AC-17 restart persistence (local) + AC-25 shutdown with SSE clients
    with group("AC-02", "restart persistence"):
        inst = Instance(binpath, work, "persist")
        storage = inst.storage
        before = None
        try:
            if check("AC-02", "persistence instance starts", inst.ready()):
                e = inst.ep()
                send_mail(e, build("From: Header Sender <header@example.test>", "Subject: persist", "X-Fixture: persist", "", "persist body"),
                          helo="persist-helo.test", mail_from="persist-env@bounce.test", rcpts=["p1@rcpt.test", "p2@rcpt.test"])
                _, lst = e.getj("/api/v1/messages")
                before = lst[0] if lst else None
                c1, c2 = SSEClient(e), SSEClient(e)
                time.sleep(0.3)
        finally:
            code, elapsed = inst.stop(15)
        check("AC-25", "SIGTERM with 2 SSE clients open -> exit 0 within 10s", code == 0 and elapsed < 10.0,
              "exit=%s elapsed=%.1fs" % (code, elapsed))
        inst = Instance(binpath, work, "persist2", storage=storage)
        try:
            if check("AC-02", "restart on same storage", inst.ready()):
                _, lst = inst.ep().getj("/api/v1/messages")
                after = lst[0] if lst else None
                check("AC-02", "message survives restart (identical compat JSON incl. ID/Created)", before is not None and after == before,
                      first_diff(after, before) or "")
                check("AC-17", "envelope From/To/Helo survive restart", after is not None and after["Raw"]["From"] == "persist-env@bounce.test" and
                      after["Raw"]["To"] == ["p1@rcpt.test", "p2@rcpt.test"] and after["Raw"]["Helo"] == "persist-helo.test")
        finally:
            inst.stop()

    # ---- AC-25 startup failures
    with group("AC-25", "startup failures"):
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            skip("AC-25", "unwritable storage is fatal", "running as root")
        else:
            ro = tempfile.mkdtemp(prefix="ro-", dir=work)
            os.chmod(ro, 0o555)
            inst = Instance(binpath, work, "readonly", storage=ro)
            code = inst.exit_within(10)
            inst.stop()
            out = inst.output()
            os.chmod(ro, 0o755)
            check("AC-25", "unwritable storage -> non-zero exit naming the uid", code not in (None, 0) and re.search(r"uid", out, re.I) is not None,
                  "exit=%s mentions-uid=%s" % (code, bool(re.search(r"uid", out, re.I))))
        st = tempfile.mkdtemp(prefix="corrupt-", dir=work)
        os.makedirs(os.path.join(st, "mail"))
        os.makedirs(os.path.join(st, "blobs"))
        marker = "CORRUPT-CONTENT-MARKER-" + secrets.token_hex(4)
        with open(os.path.join(st, "mail", "messages.jsonl"), "w") as fh:
            fh.write('{"id":"%s", this is not json\n' % marker)
        inst = Instance(binpath, work, "corrupt", storage=st)
        code = inst.exit_within(10)
        inst.stop()
        out = inst.output()
        check("AC-25", "corrupt messages.jsonl -> non-zero exit with line number, no content echoed",
              code not in (None, 0) and LINE1.search(out) is not None and marker not in out,
              "exit=%s line-number=%s content-echoed=%s" % (code, bool(LINE1.search(out)), marker in out))

    # ---- AC-22 SSE cap 64 + AC-25 slow consumer termination
    with group("AC-22", "SSE cap"):
        inst = Instance(binpath, work, "sse")
        try:
            if check("AC-22", "SSE instance starts", inst.ready()):
                e = inst.ep()
                slow = SSEClient(e, rcvbuf=4096, read=False)
                big = "y" * 20000
                send_bulk(e, [build("Subject: lag-%03d" % i, "X-Fixture: lag-%03d" % i, "", big) for i in range(300)], per_session=100)
                time.sleep(7)
                slow.start()
                closed = slow.wait(lambda c: c.closed_at is not None, 20.0)
                check("AC-25", "lagging SSE consumer is disconnected (Lagged / write timeout), not buffered forever",
                      bool(closed), "events read=%d bytes=%d" % (len(slow.events), slow.bytes_read))
                slow.close()
                clients = [SSEClient(e) for _ in range(64)]
                st64 = [c.status for c in clients]
                c65 = SSEClient(e, read=False)
                check("AC-22", "64 concurrent SSE streams accepted, 65th -> 503", st64 == [200] * 64 and c65.status == 503,
                      "accepted=%d 65th=%d" % (st64.count(200), c65.status))
                c65.close()
                for c in clients:
                    c.close()
                if SLOW:
                    time.sleep(20)
                    again = [SSEClient(e, read=False) for _ in range(64)]
                    sts = [c.status for c in again]
                    check("AC-09", "disconnected SSE clients released within one heartbeat (64 slots free after 20s) [SLOW]",
                          sts == [200] * 64, "accepted=%d" % sts.count(200))
                    for c in again:
                        c.close()
                else:
                    skip("AC-09", "disconnected SSE task ends within one heartbeat [SLOW]", "set E2E_SLOW=1")
        finally:
            inst.stop()


# --------------------------------------------------------------------------
# AC-19 performance


def perf_suite(ours):
    with group("AC-19", "performance"):
        delete_all(ours)
        blob = random.Random(19).randbytes(74000) if hasattr(random.Random, "randbytes") else os.urandom(74000)
        att = CRLF.join(x.encode() for x in b64lines(blob))
        pls = [build("From: Perf <perf@example.test>", "Subject: perf-%03d" % i, "X-Fixture: perf-%03d" % i,
                     'Content-Type: multipart/mixed; boundary="pf"', "", "--pf", "Content-Type: text/plain", "", "perf body %d" % i,
                     "--pf", 'Content-Type: application/octet-stream; name="blob.bin"', "Content-Transfer-Encoding: base64", "",
                     att, "--pf--") for i in range(300)]
        codes = send_bulk(ours, pls, per_session=50)
        check("AC-19", "300 x ~%dKB messages stored" % (len(pls[0]) // 1024), all(c == 250 for c in codes))

        def timed(path, n):
            ts = []
            last = None
            for _ in range(n):
                t0 = time.perf_counter()
                last = ours.get(path, timeout=60)
                ts.append(time.perf_counter() - t0)
            return sorted(ts)[len(ts) // 2], last
        t, r = timed("/api/v2/messages?limit=250", 3)
        ok = r.status == 200 and r.json()["count"] == 250
        check("AC-19", "GET /api/v2/messages?limit=250 < 2s (median %.3fs)" % t, ok and t < 2.0, "status=%d" % r.status)
        t, r = timed("/api/mailbox/messages?start=0&limit=50", 5)
        ok = r.status == 200 and len(r.json()["items"]) == 50
        check("AC-19", "UI list limit=50 < 300ms (median %.3fs)" % t, ok and t < 0.3, "status=%d" % r.status)
        delete_all(ours)


# --------------------------------------------------------------------------
# AC-24 sweep, AC-18 log hygiene


SPA_CSP = ["default-src 'self'", "script-src 'self'", "style-src 'self' 'unsafe-inline'", "img-src 'self' data:",
           "frame-src 'self'", "connect-src 'self'", "object-src 'none'", "base-uri 'none'", "frame-ancestors 'none'"]


def sweep_suite():
    def h(headers, name):
        for k, v in headers:
            if k.lower() == name.lower():
                return v
        return None
    nos, acao, api_csp, spa_csp = [], [], [], []
    for label, method, path, status, headers in OBSERVED:
        if (h(headers, "X-Content-Type-Options") or "").lower() != "nosniff":
            nos.append("%s %s %d" % (method, path, status))
        if any(k.lower().startswith("access-control-") for k, _ in headers):
            acao.append("%s %s" % (method, path))
        csp = h(headers, "Content-Security-Policy") or ""
        if path.startswith("/api/"):
            if "sandbox" not in csp or "default-src 'none'" not in csp:
                api_csp.append("%s %s %d" % (method, path, status))
        elif status == 200 and (path == "/" or path.startswith("/assets/") or "." not in path.rsplit("/", 1)[-1]):
            missing = [d for d in SPA_CSP if d not in csp]
            if missing:
                spa_csp.append("%s %s missing %s" % (method, path, missing[:2]))
    n = len(OBSERVED)
    check("AC-24", "X-Content-Type-Options: nosniff on every response (%d observed)" % n, not nos, "%d without: %s" % (len(nos), nos[:3]))
    check("AC-15", "no Access-Control-* header on any response (%d observed)" % n, not acao, str(acao[:3]))
    check("AC-24", "CSP 'sandbox' + default-src 'none' on every /api/* response", not api_csp, "%d without: %s" % (len(api_csp), api_csp[:3]))
    check("AC-24", "SPA responses carry the ADR CSP", not spa_csp, "%d: %s" % (len(spa_csp), spa_csp[:2]))


def log_suite(ours, args, secrets_seen):
    fake = "fake-" + secrets.token_hex(8)
    token = base64.b64encode(("leak-user:%s" % fake).encode()).decode()
    secrets_seen += [fake, token]
    try:
        for p in ["/api/v2/messages", "/api/v1/messages", "/", "/api/mailbox/messages"]:
            ours.get(p, headers=[("Authorization", "Basic " + token)])
        send_mail(ours, build("Subject: log-hygiene", "X-Fixture: log-hygiene", "", "log body " + BODY_MARKER))
    except (OSError, EOFError) as e:
        emit("FAIL", "AC-18", "log hygiene traffic could not be sent", type(e).__name__)
    time.sleep(0.5)
    logs = list(AUX_LOGS) + ([args.ours_log] if args.ours_log else [])
    leaks = []
    for path in logs:
        try:
            with open(path, "rb") as fh:
                text = fh.read().decode("utf-8", "replace")
        except OSError:
            continue
        for needle in [BODY_MARKER, "Authorization:", "Basic " + token] + secrets_seen:
            if needle in text:
                leaks.append("%s contains %s" % (os.path.basename(path), "body marker" if needle == BODY_MARKER else
                                                 "'Authorization:'" if needle == "Authorization:" else "a credential"))
    check("AC-18", "server logs (main + %d aux) contain no mail body, credentials or Authorization header" % (len(logs) - (1 if args.ours_log else 0)),
          not leaks, "; ".join(sorted(set(leaks))[:4]))
    delete_all(ours)


# --------------------------------------------------------------------------


def main():
    global SLOW
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bin", help="devcloud-mailbox binary (for auxiliary instances)")
    ap.add_argument("--ours-smtp", type=int, required=True)
    ap.add_argument("--ours-http", type=int, required=True)
    ap.add_argument("--ours-pid", type=int)
    ap.add_argument("--ours-log")
    ap.add_argument("--oracle-smtp", type=int)
    ap.add_argument("--oracle-http", type=int)
    ap.add_argument("--workdir", default=tempfile.gettempdir())
    ap.add_argument("--slow", action="store_true")
    ap.add_argument("--only", choices=["all", "compat"], default="all",
                    help="compat = MailHog API checks only (also used to self-test against two MailHog instances)")
    args = ap.parse_args()
    SLOW = args.slow
    ours = Endpoint("main", args.ours_http, args.ours_smtp, True)
    oracle = Endpoint("oracle", args.oracle_http, args.oracle_smtp, False) if args.oracle_http and args.oracle_smtp else None
    if oracle is None:
        skip("DIFF", "MailHog oracle not provided; DIFF checks are skipped")
    fixtures = build_fixtures()
    print("[e2e] fixtures: %s" % ", ".join("%s(%dB)" % (f["name"], len(f["data"])) for f in fixtures), flush=True)
    idle = None
    if SLOW and args.only == "all":
        idle = IdleSmtpWatcher(ours)
        idle.start()
    secrets_seen = []
    t0 = time.monotonic()
    def run(ac, fn, *a):
        with group(ac, "%s aborted by an unexpected error" % fn.__name__):
            fn(*a)
    run("AC-04", compat_suite, ours, oracle, fixtures, args)
    if args.only == "all":
        run("AC-10", ui_suite, ours, fixtures)
        run("AC-09", sse_suite, ours)
        run("AC-15", surface_suite, ours)
        run("AC-21", hostile_suite, ours, args)
        if args.bin:
            run("AC-14", aux_suite, args, secrets_seen)
        else:
            skip("AC-14", "auxiliary-instance checks (AC-02/14/17/22/23/25)", "no --bin")
        run("AC-19", perf_suite, ours)
        run("AC-18", log_suite, ours, args, secrets_seen)
        run("AC-24", sweep_suite)
        if idle is not None:
            left = 330 - (time.monotonic() - t0)
            if left > 0:
                print("[e2e] waiting %.0fs for the SMTP idle-timeout probe" % left, flush=True)
            idle.join(max(left, 0) + 5)
            r = idle.result
            check("AC-22", "SMTP idle connection closed at ~300s [SLOW]", r is not None and 290 <= r <= 320, "closed after %s s" % r)
        else:
            skip("AC-22", "SMTP idle timeout 300s [SLOW]", "set E2E_SLOW=1")
        info("AC-12", "iframe sandbox / script non-execution / link opens new tab are BROWSER checks (hub, claude-in-chrome)")
        info("AC-13", "dedicated UI journeys, keyboard map, aria are BROWSER checks; AC-13a contrast is BROWSER+FIX")
        info("AC-16", "covered by cargo test --workspace and VERIFY_STAGE=full scripts/mail-autoloop/verify.sh (loop oracle 1,5)")
        info("AC-20", "README content is a judge (read) check")
        info("AC-28", "covered by cargo test --workspace (dashboard_conformance) (loop oracle 1)")
    print("[e2e] harness summary: PASS=%d FAIL=%d SKIP=%d INFO=%d (%.0fs)" % (
        COUNTS["PASS"], COUNTS["FAIL"], COUNTS["SKIP"], COUNTS["INFO"], time.monotonic() - t0), flush=True)
    for f in FAILED:
        print("[e2e]   failed: %s" % f, flush=True)
    return 1 if COUNTS["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
