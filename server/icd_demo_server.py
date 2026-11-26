#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Live ICD-10-CM demo backend.

One clinical note in; the stages of the pipeline run on locally served models and
the result comes back in the form the review page reads (note, extracted concepts
with located quotations, ranked candidates, suggested codes).
Standard library only (Python 3.8+), so it runs without installing anything.

Pipelines (ICD_DEMO_PIPELINE):
  ERG  Extract -> Retrieve -> Generate, the full chain of the experiments.
       Extract and Generate call the generation model through an OpenAI-compatible
       /v1/chat/completions endpoint with the prompts of the experiments. Retrieve
       asks the re-ranker service (rerank_service.py) to score every code of the
       132-code label list against the note and keeps the 50 best, then scores
       those 50 against each extracted query to link codes to concepts. (The
       experiments first pooled the hits of a hierarchy and an index search and
       re-ranked that pool; on this label list the pool held nearly every code,
       so the live demo re-ranks the list directly.)
  G    Generate only: the note and the label list. Needs no re-ranker.

Privacy rules this file keeps:
  * the note and the model responses live in memory only and are never written
    to a file or a log line (log lines carry time, method, route and status);
  * a finished job is dropped as soon as the page has read it;
  * the model endpoints must resolve to a loopback or private address, so a
    note cannot be sent to an outside API by a wrong setting;
  * browsers are limited to the listed page origins, and the server listens
    on this machine only unless told otherwise. An access token is optional:
    by default anyone who can reach the server can use it, and access is
    controlled by turning the server off.

Settings are environment variables (see demo.env.example).
"""

import hashlib
import hmac
import http.client
import ipaddress
import json
import os
import queue
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import unicodedata
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

HERE = os.path.dirname(os.path.abspath(__file__))
SITE = os.path.dirname(HERE)


def env(name, default):
    value = os.environ.get(name)
    return default if value is None or value.strip() == "" else value.strip()


HOST = env("ICD_DEMO_HOST", "127.0.0.1")
PORT = int(env("ICD_DEMO_PORT", "8765"))
PIPELINE = env("ICD_DEMO_PIPELINE", "ERG").upper().replace("-", "").replace("_", "")
LLM_URL = env("ICD_DEMO_LLM_URL", "http://127.0.0.1:8000/v1").rstrip("/")
LLM_MODEL = env("ICD_DEMO_LLM_MODEL", "")
LLM_API_KEY = env("ICD_DEMO_LLM_API_KEY", "")
RERANK_URL = env("ICD_DEMO_RERANK_URL", "http://127.0.0.1:8766").rstrip("/")
RERANK_API_KEY = env("ICD_DEMO_RERANK_API_KEY", "")
CANDIDATES = int(env("ICD_DEMO_CANDIDATES", "50"))
SHOWN_PER_CONCEPT = 5
HINT_ROWS_RANK = 10
MAX_ITEMS = int(env("ICD_DEMO_MAX_ITEMS", "40"))
MAX_NOTE_CHARS = int(env("ICD_DEMO_MAX_NOTE_CHARS", "20000"))
MAX_TOKENS = int(env("ICD_DEMO_MAX_TOKENS", "16384"))
TIMEOUT_S = int(env("ICD_DEMO_TIMEOUT_S", "600"))
FAST_MAX_TOKENS = int(env("ICD_DEMO_FAST_MAX_TOKENS", "4096"))
FAST_TIMEOUT_S = int(env("ICD_DEMO_FAST_TIMEOUT_S", "180"))
RERANK_TIMEOUT_S = int(env("ICD_DEMO_RERANK_TIMEOUT_S", "300"))
QUEUE_MAX = int(env("ICD_DEMO_QUEUE_MAX", "8"))
JOB_TTL_S = int(env("ICD_DEMO_JOB_TTL_S", "900"))
PIDFILE = env("ICD_DEMO_PIDFILE", "")
# 0 keeps the server up. N > 0 stops it after N minutes without a note to process.
IDLE_EXIT_MIN = float(env("ICD_DEMO_IDLE_EXIT_MIN", "0"))
# Optional shell command that stops the model services; run when the server stops itself for idleness.
MODEL_STOP_CMD = env("ICD_DEMO_MODEL_STOP_CMD", "")

if PIPELINE not in ("ERG", "G"):
    sys.exit("ICD_DEMO_PIPELINE must be ERG or G")
PIPELINE_NAME = {"ERG": "E-R-G", "G": "G"}[PIPELINE]
STAGES = {"ERG": ["extract", "retrieve", "generate"], "G": ["generate"]}[PIPELINE]

# Two ways to run a note.
#   full: the setting of the experiments. The model reasons before it answers
#         (temperature 0.6, top-p 0.95, top-k 20, seed 42).
#   fast: reasoning is switched off, so only the answer is generated
#         (the model's recommended non-reasoning sampling). Not evaluated.
MODES = {
    "full": {"sampling": {"temperature": 0.6, "top_p": 0.95}, "extra": {"top_k": 20, "seed": 42},
             "max_tokens": MAX_TOKENS, "timeout": TIMEOUT_S},
    "fast": {"sampling": {"temperature": 0.7, "top_p": 0.8},
             "extra": {"top_k": 20, "seed": 42, "chat_template_kwargs": {"enable_thinking": False}},
             "max_tokens": FAST_MAX_TOKENS, "timeout": FAST_TIMEOUT_S},
}
DEFAULT_MODE = env("ICD_DEMO_DEFAULT_MODE", "full")
if DEFAULT_MODE not in MODES:
    sys.exit("ICD_DEMO_DEFAULT_MODE must be full or fast")

CATALOG_NOTE = "ICD-10-CM FY2022 official titles (two codes from the April 2025 release) · 132-code benchmark label list"
CONTEXT_TAGS = ("uncertain", "history", "family_history", "negated")

DEFAULT_ORIGINS = "https://icd-coding.netlify.app"
ORIGINS = set(o.strip().rstrip("/") for o in env("ICD_DEMO_ORIGINS", DEFAULT_ORIGINS).split(",") if o.strip())
# The page can also be opened from this server itself.
for _h in ("127.0.0.1", "localhost"):
    ORIGINS.add("http://%s:%d" % (_h, PORT))

MAX_BODY_BYTES = MAX_NOTE_CHARS * 4 + 4096
STATIC_PAGES = {
    "/": "index_kr.html",
    "/index_kr.html": "index_kr.html",
    "/index_en.html": "index_en.html",
    "/demo": "demo.html",
    "/demo.html": "demo.html",
    "/demo_en": "demo_en.html",
    "/demo_en.html": "demo_en.html",
}


def log(*parts):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), *parts, flush=True)


# ---------------------------------------------------------------- start-up checks

def load_token():
    """No token by default. ICD_DEMO_TOKEN sets one; ICD_DEMO_REQUIRE_TOKEN=1 uses server/.access_token."""
    token = os.environ.get("ICD_DEMO_TOKEN", "").strip()
    if token:
        return token
    if os.environ.get("ICD_DEMO_REQUIRE_TOKEN") != "1":
        return ""
    path = os.path.join(HERE, ".access_token")
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as handle:
            token = handle.read().strip()
        if token:
            return token
    sys.exit("ICD_DEMO_REQUIRE_TOKEN=1 but there is no token. Start with server/demo.sh "
             "(it creates server/.access_token) or set ICD_DEMO_TOKEN.")


def is_loopback(host):
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host == "localhost"


def require_internal(url, name):
    """Stop if a model endpoint is outside the machine or private network."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        sys.exit("%s must look like http://127.0.0.1:8000/v1" % name)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(parts.hostname, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        sys.exit("%s host cannot be resolved." % name)
    for info in infos:
        address = ipaddress.ip_address(info[4][0].split("%")[0])
        if address.is_global:
            sys.exit("Refusing to start: %s resolves to a public address. "
                     "Notes may only go to a model served on this machine or the private network." % name)


def read_text(name):
    with open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def load_prompts():
    generate = read_text("system_prompt_aci132.txt")
    start = generate.index("<GLOBAL_CODE_CATALOG>") + len("<GLOBAL_CODE_CATALOG>")
    end = generate.rindex("</GLOBAL_CODE_CATALOG>")
    catalog = []
    for row in json.loads(generate[start:end]):
        catalog.append((row["code"], row["title"]))
    return generate, read_text("system_prompt_extract.txt"), catalog


GENERATE_PROMPT, EXTRACT_PROMPT, CATALOG_ROWS = load_prompts()
CATALOG = dict(CATALOG_ROWS)
UNDOTTED = dict((code.replace(".", ""), code) for code in CATALOG)
TOKEN = ""


# ---------------------------------------------------------------- model endpoints

class DemoError(Exception):
    def __init__(self, code, message, status=None):
        Exception.__init__(self, message)
        self.code = code
        self.message = message
        self.status = status


def http_call(base, key, method, path, body=None, timeout=30, job=None, what="LLM"):
    parts = urlsplit(base)
    connection_class = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    connection = connection_class(parts.hostname, parts.port, timeout=timeout)
    if job is not None:
        job.connection = connection
    headers = {"Accept": "application/json"}
    if key:
        headers["Authorization"] = "Bearer " + key
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    try:
        connection.request(method, (parts.path or "") + path, body=data, headers=headers)
        response = connection.getresponse()
        return response.status, response.read()
    except socket.timeout:
        raise DemoError(what + "_TIMEOUT", "A model did not answer within %d seconds." % timeout)
    except (OSError, http.client.HTTPException):
        raise DemoError(what + "_UNREACHABLE", "A model service cannot be reached.")
    finally:
        if job is not None:
            job.connection = None
        connection.close()


def llm_call(method, path, body=None, timeout=30, job=None):
    return http_call(LLM_URL, LLM_API_KEY, method, path, body, timeout, job, "LLM")


_model_cache = {"at": 0.0, "id": None}
_rerank_cache = {"at": 0.0, "ok": False}
_cache_lock = threading.Lock()


def served_model(max_age=15):
    """Model id reported by the endpoint (or the configured one), None if it is down."""
    with _cache_lock:
        if time.time() - _model_cache["at"] < max_age:
            return _model_cache["id"]
    model = None
    try:
        status, raw = llm_call("GET", "/models", timeout=4)
        if LLM_MODEL:
            model = LLM_MODEL  # the endpoint answered, so the configured name is used as is
        elif status == 200:
            ids = [row.get("id") for row in json.loads(raw.decode("utf-8")).get("data", []) if row.get("id")]
            if ids:
                model = ids[0]
    except (DemoError, ValueError, AttributeError):
        model = None
    with _cache_lock:
        _model_cache["at"] = time.time()
        _model_cache["id"] = model
    return model


def rerank_ready(max_age=15):
    if PIPELINE != "ERG":
        return True
    with _cache_lock:
        if time.time() - _rerank_cache["at"] < max_age:
            return _rerank_cache["ok"]
    ok = False
    try:
        status, raw = http_call(RERANK_URL, RERANK_API_KEY, "GET", "/health", timeout=4, what="RERANK")
        ok = status == 200 and bool(json.loads(raw.decode("utf-8")).get("ready"))
    except (DemoError, ValueError, AttributeError):
        ok = False
    with _cache_lock:
        _rerank_cache["at"] = time.time()
        _rerank_cache["ok"] = ok
    return ok


def last_json_with(text, key):
    """The value under `key` in the last JSON object of an answer that has it, or None."""
    cut = text.rfind("</think>")
    if cut >= 0:
        text = text[cut + len("</think>"):]
    decoder = json.JSONDecoder()
    for match in reversed(list(re.finditer(r"\{", text))):
        try:
            value, _ = decoder.raw_decode(text, match.start())
        except ValueError:
            continue
        if isinstance(value, dict) and isinstance(value.get(key), list):
            return value[key]
    return None


def chat(job, model, system, user):
    """One generation call. Returns (answer text, reasoning was used, answer was cut off)."""
    mode = MODES[job.mode]
    body = {
        "model": model,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "max_tokens": mode["max_tokens"],
        "stream": False,
    }
    body.update(mode["sampling"])
    body.update(mode["extra"])
    # An endpoint may refuse the token budget (context too small) or the extra
    # fields with HTTP 400. Fall back step by step: first let the endpoint use
    # its own token limit, then send the standard fields only. In the last
    # case a fast request may be answered with reasoning; the result says so.
    without_budget = dict(body)
    without_budget.pop("max_tokens")
    standard_only = dict((k, v) for k, v in without_budget.items() if k not in mode["extra"])
    status, raw = 0, b""
    for attempt in (body, without_budget, standard_only):
        status, raw = llm_call("POST", "/chat/completions", attempt, timeout=mode["timeout"], job=job)
        if status != 400 or job.cancelled:
            break
    if status != 200:
        raise DemoError("LLM_HTTP_ERROR", "The generation endpoint answered HTTP %d." % status)
    try:
        choice = json.loads(raw.decode("utf-8"))["choices"][0]
        message = choice.get("message") or {}
        text = message.get("content") or ""
        reasoning = message.get("reasoning_content") or message.get("reasoning") or ""
    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
        raise DemoError("LLM_BAD_RESPONSE", "The generation endpoint returned an unexpected body.")
    inline = re.search(r"<think>(.*?)(</think>|$)", text, re.S)
    reasoned = bool(str(reasoning).strip()) or bool(inline and inline.group(1).strip())
    return text, reasoned, choice.get("finish_reason") == "length"


# ---------------------------------------------------------------- Extract

def _normalized(text, drop_punctuation):
    """Lower-cased text with runs of white space as one space, plus the original index of every kept character."""
    chars, index = [], []
    pending_space = False
    for i, ch in enumerate(text):
        if ch.isspace():
            pending_space = bool(chars)
            continue
        if drop_punctuation and unicodedata.category(ch)[0] in ("P", "S"):
            continue
        folded = ch.casefold()
        if pending_space:
            chars.append(" ")
            index.append(i)
            pending_space = False
        for piece in folded:
            chars.append(piece)
            index.append(i)
    return "".join(chars), index


def _find_fragment(note, views, fragment):
    """(start, end) of the first place where the fragment stands in the note, or None."""
    fragment = fragment.strip()
    if not fragment:
        return None
    at = note.find(fragment)
    if at >= 0:
        return at, at + len(fragment)
    for drop_punctuation in (False, True):
        needle = _normalized(fragment, drop_punctuation)[0]
        if len(needle) < 3:
            continue
        haystack, index = views[drop_punctuation]
        at = haystack.find(needle)
        if at >= 0:
            return index[at], index[at + len(needle) - 1] + 1
    return None


def locate_quote(note, quote, views=None):
    """Spans of a quotation in the note, or None when it is not there.

    Exact matching first; then tolerant of case and white space; then of punctuation as well.
    A quotation written with an ellipsis is located fragment by fragment, and every fragment must be found.
    Nothing is matched approximately, so a located span always shows text that is in the note.
    """
    if views is None:
        views = {False: _normalized(note, False), True: _normalized(note, True)}
    whole = _find_fragment(note, views, quote)
    if whole is not None:
        return [whole]
    fragments = [f for f in re.split(r"\.{3,}|…", quote) if f.strip()]
    if len(fragments) < 2:
        return None
    spans = []
    for fragment in fragments:
        if len(fragment.split()) < 2:
            return None
        span = _find_fragment(note, views, fragment)
        if span is None:
            return None
        spans.append(span)
    return spans


def run_extract(job, model):
    text, reasoned, truncated = chat(job, model, EXTRACT_PROMPT,
                                     "<NOTE>\n" + job.note + "\n</NOTE>\n\nList the items for this note as specified.\n")
    rows = last_json_with(text, "queries")
    if rows is None:
        raise DemoError("EXTRACT_NO_ANSWER_TRUNCATED" if truncated else "EXTRACT_NO_ANSWER",
                        "The extraction answer did not contain a list of items.")
    views = {False: _normalized(job.note, False), True: _normalized(job.note, True)}
    items = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        concept, query = row.get("concept"), row.get("query")
        if not isinstance(concept, str) or not isinstance(query, str) or not concept.strip() or not query.strip():
            continue
        evidence = []
        for quote in row.get("evidence") or []:
            if not isinstance(quote, str) or not quote.strip():
                continue
            spans = locate_quote(job.note, quote, views)
            entry = {"quote": quote, "linked": spans is not None}
            if spans:
                entry["start"], entry["end"] = spans[0]
                if len(spans) > 1:
                    entry["spans"] = [{"start": a, "end": b} for a, b in spans]
            evidence.append(entry)
        tags = [t for t in (row.get("context_tags") or []) if t in CONTEXT_TAGS]
        items.append({"id": "q%02d" % (len(items) + 1), "concept": concept.strip(), "query": query.strip(),
                      "evidence": evidence, "tags": tags, "candidates": []})
        if len(items) >= MAX_ITEMS:
            break
    return items, reasoned, truncated


# ---------------------------------------------------------------- Retrieve

def rerank_scores(job, pairs):
    status, raw = http_call(RERANK_URL, RERANK_API_KEY, "POST", "/score", {"pairs": pairs},
                            timeout=RERANK_TIMEOUT_S, job=job, what="RERANK")
    if status == 413:
        raise DemoError("NOTE_TOO_LONG_FOR_RERANK", "The note is longer than the re-ranker accepts.")
    if status != 200:
        raise DemoError("RERANK_HTTP_ERROR", "The re-ranker answered HTTP %d." % status)
    try:
        scores = json.loads(raw.decode("utf-8"))["scores"]
        assert len(scores) == len(pairs) and all(isinstance(x, (int, float)) for x in scores)
    except (ValueError, KeyError, TypeError, AssertionError):
        raise DemoError("RERANK_BAD_RESPONSE", "The re-ranker returned an unexpected body.")
    return scores


def ranks_of(scores):
    """Rank 1 for the highest score; ties are broken by the order of the label list (code ascending)."""
    order = sorted(range(len(scores)), key=lambda i: (-scores[i], CATALOG_ROWS[i][0]))
    ranks = [0] * len(scores)
    for position, i in enumerate(order):
        ranks[i] = position + 1
    return order, ranks


def run_retrieve(job, items):
    """Score every code of the label list against the note and keep the best CANDIDATES;
    then score those candidates against each extracted query, to know which concept supports which code."""
    titles = [title for _, title in CATALOG_ROWS]
    note_scores = rerank_scores(job, [[job.note, title] for title in titles])
    top = ranks_of(note_scores)[0][:CANDIDATES]
    per_query = []
    if items:
        scores = rerank_scores(job, [[item["query"], titles[i]] for item in items for i in top])
        m = len(top)
        for k in range(len(items)):
            part = scores[k * m:(k + 1) * m]
            order = sorted(range(m), key=lambda j: (-part[j], CATALOG_ROWS[top[j]][0]))
            rank = [0] * m
            for position, j in enumerate(order):
                rank[j] = position + 1
            per_query.append((part, rank))
    candidates = []
    for position, i in enumerate(top):
        code, title = CATALOG_ROWS[i]
        supports = [(k, per_query[k][1][position], per_query[k][0][position]) for k in range(len(items))]
        # the concept whose query ranks this code best among the candidates; the earlier concept on ties
        best = min(supports, key=lambda s: (s[1], s[0]))[0] if supports else None
        candidates.append({"code": code, "title": title, "rank": position + 1, "score": note_scores[i],
                           "supports": supports, "concept": best})
    return candidates


# ---------------------------------------------------------------- Generate

def hint_block(items, candidates):
    """The retrieval hints, in the layout the experiments gave to the generator."""
    def number(value):
        return float("%.8g" % value)

    rows = []
    for c in candidates:
        rows.append({"code": c["code"], "kind": "diagnosis", "official_title": c["title"],
                     # only the queries that rank this code among their best ones, to keep the prompt short
                     "query_support_rows": [[k, rank, number(score), 0] for k, rank, score in c["supports"] if rank <= HINT_ROWS_RANK],
                     "rank": c["rank"], "score": number(c["score"]),
                     "score_kind": "qwen3_reranker_native_yes_no", "system": "ICD10:CM"})
    listed = []
    for item in items:
        spans, shown = [], 0
        for index, ev in enumerate(item["evidence"]):
            if not ev["linked"]:
                continue
            for span in ev.get("spans") or [ev]:
                shown += 1
                spans.append({"display_index": shown, "end": span["end"], "source_record_indices": [index], "start": span["start"]})
        listed.append({"concept": item["concept"], "context_tags": item["tags"], "evidence": [ev["quote"] for ev in item["evidence"]],
                       "evidence_spans": spans, "kind": "diagnosis", "query": item["query"], "query_id": item["id"]})
    block = {
        "allowed_output_scope_changed": False,
        "candidates": rows,
        "extra_ontology_context": False,
        "items": listed,
        "kind": "retrieval_hints_not_verified_clinical_evidence",
        "query_support_tables": {
            "constant_score_kind": None,
            "meaning": "Each candidate query_support_rows entry resolves query and score_kind indexes using these shared tables.",
            "query_ids": [item["id"] for item in items],
            "score_kinds": ["qwen3_reranker_native_yes_no_query_to_official_title"],
            "support_fields": ["query_id_index", "retrieval_rank", "retrieval_score", "score_kind_index"],
        },
        "retrieval_score_encoding": {
            "meaning": "Scores are plain decimal numbers. Higher retrieval scores mean greater similarity, not clinical confidence.",
            "name": "plain_decimal",
        },
        "route": "E_R_G",
    }
    return json.dumps(block, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def tidy_codes(raw_codes):
    codes, seen = [], set()
    for item in raw_codes:
        code = item.get("code") if isinstance(item, dict) else item
        if not isinstance(code, str):
            continue
        code = code.strip().upper()
        if not re.match(r"^[A-Z0-9][A-Z0-9.]{1,10}$", code):
            continue
        if code not in CATALOG and code.replace(".", "") in UNDOTTED:
            code = UNDOTTED[code.replace(".", "")]
        if code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def run_generate(job, model, items, candidates):
    user = "<TARGET_NOTE>\n" + job.note + "\n</TARGET_NOTE>\nReturn the ICD code set in the declared JSON schema."
    if PIPELINE == "ERG":
        user = "<ADDITIONAL_INFORMATION>\n" + hint_block(items, candidates) + "\n</ADDITIONAL_INFORMATION>\n" + user
    text, reasoned, truncated = chat(job, model, GENERATE_PROMPT, user)
    raw_codes = last_json_with(text, "codes")
    if raw_codes is None:
        raise DemoError("NO_ANSWER_TRUNCATED" if truncated else "NO_ANSWER",
                        "The model response did not contain a code list.")
    return tidy_codes(raw_codes), reasoned, truncated


# ---------------------------------------------------------------- result in the form the review page reads

def build_example(note, items, candidates, codes):
    by_id = dict((item["id"], item) for item in items)
    link, title = {}, {}
    for c in candidates:
        title[c["code"]] = c["title"]
        if c["concept"] is None:
            continue
        concept_id = items[c["concept"]]["id"]
        link[c["code"]] = concept_id
        by_id[concept_id]["candidates"].append({"code": c["code"], "rank": c["rank"], "title": c["title"]})
    final = set(codes)
    for item in items:
        item["candidates"] = [c for n, c in enumerate(item["candidates"]) if n < SHOWN_PER_CONCEPT or c["code"] in final]
    cards = []
    for code in codes:
        concept_id = link.get(code)
        cards.append({
            "code": code,
            "title": title.get(code) or CATALOG.get(code) or "Not in the 132-code label list",
            "item_ids": [concept_id] if concept_id else [],
            "evidence": [dict(ev) for ev in by_id[concept_id]["evidence"]] if concept_id else [],
            "in_label_list": code in CATALOG,
        })
    return {
        "id": "live",
        "title": "",
        "note": note,
        "items": items,
        "codes": cards,
        "candidate_count": len(candidates),
        "label_space": len(CATALOG),
        "catalog": CATALOG_NOTE,
        "output_kind": "live",
        "pipeline_status": "COMPLETE",
        "performance_evaluation": False,
        "source": {"label": "", "note_sha256": hashlib.sha256(note.encode("utf-8")).hexdigest()},
    }


def run_pipeline(job):
    started = time.time()
    model = served_model(max_age=0)
    if not model:
        raise DemoError("LLM_UNREACHABLE", "The generation endpoint cannot be reached.")
    seconds, reasoned_any, truncated_any = {}, False, False
    items, candidates = [], []

    def stage(name):
        job.stage = name
        return time.time()

    if PIPELINE == "ERG":
        tick = stage("extract")
        items, reasoned, truncated = run_extract(job, model)
        reasoned_any, truncated_any = reasoned_any or reasoned, truncated_any or truncated
        seconds["extract"] = round(time.time() - tick, 1)
        job.info = {"concepts": len(items)}
        tick = stage("retrieve")
        candidates = run_retrieve(job, items)
        seconds["retrieve"] = round(time.time() - tick, 1)
        job.info = {"concepts": len(items), "candidates": len(candidates)}
    tick = stage("generate")
    codes, reasoned, truncated = run_generate(job, model, items, candidates)
    reasoned_any, truncated_any = reasoned_any or reasoned, truncated_any or truncated
    seconds["generate"] = round(time.time() - tick, 1)
    return {
        "example": build_example(job.note, items, candidates, codes),
        "model": model,
        "pipeline": PIPELINE_NAME,
        "mode": job.mode,
        "reasoning_used": reasoned_any,
        "truncated": truncated_any,
        "label_space": len(CATALOG),
        "stage_seconds": seconds,
        "elapsed_s": round(time.time() - started, 1),
    }


# ---------------------------------------------------------------- jobs (memory only)

class Job(object):
    def __init__(self, note, mode):
        self.id = uuid.uuid4().hex
        self.note = note
        self.mode = mode
        self.status = "queued"
        self.stage = None
        self.info = {}
        self.created = time.time()
        self.started = None
        self.finished = None
        self.result = None
        self.error = None
        self.cancelled = False
        self.connection = None


JOBS = {}
JOBS_LOCK = threading.Lock()
WAITING = queue.Queue()
LAST_ACTIVITY = [time.time()]
IDLE_STOP = [False]


def worker():
    while True:
        job = WAITING.get()
        with JOBS_LOCK:
            if job.cancelled:
                job.note = None
                continue
            job.status = "running"
            job.started = time.time()
        result, error = None, None
        try:
            result = run_pipeline(job)
        except DemoError as exc:
            error = {"code": exc.code, "message": exc.message, "stage": job.stage}
        except Exception as exc:  # never let the note or the answer reach a log line
            error = {"code": "INTERNAL", "message": "Internal error (%s)." % type(exc).__name__, "stage": job.stage}
        with JOBS_LOCK:
            job.note = None
            job.finished = time.time()
            if job.cancelled:
                continue
            job.result, job.error = result, error
            job.status = "done" if error is None else "error"
            LAST_ACTIVITY[0] = time.time()
        log("job", job.status, "-" if error is None else error["code"])


def idle_watch(httpd):
    """Stop the server after IDLE_EXIT_MIN minutes without a note to process."""
    while True:
        time.sleep(min(15.0, max(1.0, IDLE_EXIT_MIN * 15)))
        with JOBS_LOCK:
            active = any(j.status in ("queued", "running") for j in JOBS.values())
        if active:
            LAST_ACTIVITY[0] = time.time()
        elif time.time() - LAST_ACTIVITY[0] > IDLE_EXIT_MIN * 60:
            log("no note for %g min: stopping" % IDLE_EXIT_MIN)
            IDLE_STOP[0] = True
            httpd.shutdown()
            return


def sweep_jobs():
    now = time.time()
    with JOBS_LOCK:
        for job_id in [k for k, j in JOBS.items() if j.finished and now - j.finished > JOB_TTL_S]:
            del JOBS[job_id]


def cancel_job(job):
    """Caller holds JOBS_LOCK."""
    job.cancelled = True
    job.note = None
    JOBS.pop(job.id, None)
    connection = job.connection
    if connection is not None and connection.sock is not None:
        try:
            connection.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


# ---------------------------------------------------------------- HTTP

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "icd-demo"
    sys_version = ""
    timeout = 30

    def log_message(self, fmt, *args):  # the default would print raw request lines
        pass

    def log_error(self, fmt, *args):
        pass

    # -- helpers
    def origin(self):
        value = self.headers.get("Origin")
        return value.rstrip("/") if value else None

    def cors_headers(self):
        origin = self.origin()
        headers = [("Vary", "Origin")]
        if origin in ORIGINS:
            headers.append(("Access-Control-Allow-Origin", origin))
        return headers

    def reply(self, status, payload=None, route="other", extra=(), content_type="application/json; charset=utf-8"):
        if payload is None:
            body = b""
        elif isinstance(payload, bytes):
            body = payload
        else:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        if body:
            self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        for name, value in list(self.cors_headers()) + list(extra):
            self.send_header(name, value)
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)
        log(self.command, route, status)

    def fail(self, status, code, message, route, **more):
        error = {"code": code, "message": message}
        error.update(more)
        self.reply(status, {"error": error}, route)

    def authorized(self):
        if not TOKEN:
            return True
        given = self.headers.get("Authorization", "")
        return hmac.compare_digest(given.encode("utf-8"), ("Bearer " + TOKEN).encode("utf-8"))

    def api_guard(self, route):
        """Origin and token checks shared by the API routes. True when the request may go on."""
        origin = self.origin()
        if origin is not None and origin not in ORIGINS:
            self.fail(403, "ORIGIN_NOT_ALLOWED", "This page origin is not on the server's allow list.", route)
            return False
        if not self.authorized():
            self.fail(401, "UNAUTHORIZED", "A valid access token is required.", route)
            return False
        return True

    # -- methods
    def do_OPTIONS(self):
        origin = self.origin()
        if origin not in ORIGINS or not self.path.startswith("/api/"):
            self.reply(403, None, "preflight")
            return
        extra = [
            ("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS"),
            ("Access-Control-Allow-Headers", "Authorization, Content-Type"),
            ("Access-Control-Max-Age", "600"),
        ]
        if self.headers.get("Access-Control-Request-Private-Network"):
            extra.append(("Access-Control-Allow-Private-Network", "true"))
        self.reply(204, None, "preflight", extra)

    def do_GET(self):
        sweep_jobs()
        path = urlsplit(self.path).path
        if path == "/api/health":
            self.get_health()
        elif path.startswith("/api/jobs/"):
            self.get_job(path[len("/api/jobs/"):])
        elif path in STATIC_PAGES:
            self.get_page(STATIC_PAGES[path])
        else:
            self.fail(404, "NOT_FOUND", "No such route.", "other")

    def do_POST(self):
        sweep_jobs()
        if urlsplit(self.path).path == "/api/predict":
            self.post_predict()
        else:
            self.close_connection = True
            self.fail(404, "NOT_FOUND", "No such route.", "other")

    def do_DELETE(self):
        path = urlsplit(self.path).path
        route = "/api/jobs/<id>"
        if not path.startswith("/api/jobs/"):
            self.fail(404, "NOT_FOUND", "No such route.", "other")
            return
        if not self.api_guard(route):
            return
        with JOBS_LOCK:
            job = JOBS.get(path[len("/api/jobs/"):])
            if job is not None:
                cancel_job(job)
        self.reply(200, {"cancelled": job is not None}, route)

    # -- routes
    def get_health(self):
        route = "/api/health"
        origin = self.origin()
        if origin is not None and origin not in ORIGINS:
            self.fail(403, "ORIGIN_NOT_ALLOWED", "This page origin is not on the server's allow list.", route)
            return
        payload = {"ok": True, "auth_required": bool(TOKEN), "authorized": self.authorized()}
        if payload["authorized"]:
            model = served_model()
            reranker = rerank_ready()
            with JOBS_LOCK:
                waiting = sum(1 for j in JOBS.values() if j.status == "queued")
                running = sum(1 for j in JOBS.values() if j.status == "running")
            payload.update({
                "llm_ready": model is not None,
                "rerank_ready": reranker,
                "ready": model is not None and reranker,
                "model": model,
                "pipeline": PIPELINE_NAME,
                "stages": STAGES,
                "label_space": len(CATALOG),
                "candidates": CANDIDATES if PIPELINE == "ERG" else 0,
                "max_note_chars": MAX_NOTE_CHARS,
                "default_mode": DEFAULT_MODE,
                "queue": {"waiting": waiting, "running": running},
                "idle_exit_min": IDLE_EXIT_MIN,
            })
            if IDLE_EXIT_MIN > 0:
                left = IDLE_EXIT_MIN * 60 - (time.time() - LAST_ACTIVITY[0])
                payload["idle_exit_in_s"] = 0 if waiting or running else max(0, int(left))
        self.reply(200, payload, route)

    def post_predict(self):
        route = "/api/predict"
        keep_alive = not self.close_connection
        self.close_connection = True  # any early return below may leave the body unread
        if not self.api_guard(route):
            return
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self.fail(411, "BAD_REQUEST", "Content-Length is required.", route)
            return
        if length > MAX_BODY_BYTES:
            self.fail(413, "NOTE_TOO_LONG", "The note is longer than the server accepts.", route,
                      max_note_chars=MAX_NOTE_CHARS)
            return
        try:
            request = json.loads(self.rfile.read(length).decode("utf-8"))
            note = request.get("note")
            mode = request.get("mode") or DEFAULT_MODE
        except (ValueError, AttributeError, OSError):
            self.fail(400, "BAD_REQUEST", "The body must be JSON like {\"note\": \"...\"}.", route)
            return
        self.close_connection = not keep_alive
        if not isinstance(note, str) or not note.strip():
            self.fail(400, "NOTE_EMPTY", "The note is empty.", route)
            return
        if mode not in MODES:
            self.fail(400, "BAD_REQUEST", "mode must be full or fast.", route)
            return
        note = note.replace("\r\n", "\n").replace("\r", "\n").strip("\n")
        if len(note) > MAX_NOTE_CHARS:
            self.fail(413, "NOTE_TOO_LONG", "The note is longer than the server accepts.", route,
                      max_note_chars=MAX_NOTE_CHARS)
            return
        with JOBS_LOCK:
            active = sum(1 for j in JOBS.values() if j.status in ("queued", "running"))
            if active >= QUEUE_MAX:
                job = None
            else:
                job = Job(note, mode)
                JOBS[job.id] = job
                LAST_ACTIVITY[0] = time.time()
        if job is None:
            self.fail(429, "QUEUE_FULL", "Too many notes are waiting. Try again shortly.", route)
            return
        WAITING.put(job)
        self.reply(202, {"job_id": job.id, "stages": STAGES}, route)

    def get_job(self, job_id):
        route = "/api/jobs/<id>"
        if not self.api_guard(route):
            return
        now = time.time()
        with JOBS_LOCK:
            job = JOBS.get(job_id)
            if job is None:
                payload = None
            else:
                payload = {"status": job.status}
                if job.status == "queued":
                    payload["ahead"] = sum(1 for j in JOBS.values()
                                           if j.status in ("queued", "running") and j.created < job.created)
                    payload["elapsed_s"] = round(now - job.created, 1)
                elif job.status == "running":
                    payload["elapsed_s"] = round(now - job.started, 1)
                    payload["stage"] = job.stage
                    payload["info"] = dict(job.info)
                else:
                    if job.status == "done":
                        payload["result"] = job.result
                    else:
                        payload["error"] = job.error
                    del JOBS[job_id]  # read once, then gone
        if payload is None:
            self.fail(404, "JOB_NOT_FOUND", "No such job. It may have been read already or expired.", route)
        else:
            self.reply(200, payload, route)

    def get_page(self, name):
        path = os.path.join(SITE, name)
        try:
            with open(path, "rb") as handle:
                body = handle.read()
        except OSError:
            self.fail(404, "NOT_FOUND", "No such page.", "page")
            return
        self.reply(200, body, "page", content_type="text/html; charset=utf-8")


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    global TOKEN
    require_internal(LLM_URL, "ICD_DEMO_LLM_URL")
    if PIPELINE == "ERG":
        require_internal(RERANK_URL, "ICD_DEMO_RERANK_URL")
    TOKEN = load_token()
    if not TOKEN and not is_loopback(HOST):
        sys.exit("Refusing to listen on %s without an access token." % HOST)
    httpd = Server((HOST, PORT), Handler)
    if PIDFILE:
        with open(PIDFILE, "w", encoding="utf-8") as handle:
            handle.write(str(os.getpid()))
    threading.Thread(target=worker, name="worker", daemon=True).start()
    if IDLE_EXIT_MIN > 0:
        threading.Thread(target=idle_watch, args=(httpd,), name="idle", daemon=True).start()

    def on_signal(signum, frame):
        raise KeyboardInterrupt()

    for name in ("SIGTERM", "SIGHUP"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), on_signal)
    log("listening on http://%s:%d" % (HOST, PORT))
    log("pipeline", PIPELINE_NAME, "label list", len(CATALOG), "codes", "token", "on" if TOKEN else "off",
        "default mode", DEFAULT_MODE)
    log("generation endpoint", LLM_URL, "model", LLM_MODEL or "(first served model)")
    if PIPELINE == "ERG":
        log("re-ranker endpoint", RERANK_URL, "candidates kept", CANDIDATES)
    log("allowed origins", " ".join(sorted(ORIGINS)))
    log("idle stop", ("after %g min without a note" % IDLE_EXIT_MIN) if IDLE_EXIT_MIN > 0 else "off")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        if PIDFILE:
            try:
                os.remove(PIDFILE)
            except OSError:
                pass
        if IDLE_STOP[0] and MODEL_STOP_CMD:
            log("running the model stop command")
            subprocess.call(MODEL_STOP_CMD, shell=True, stdin=subprocess.DEVNULL)
        log("stopped")
    if IDLE_STOP[0]:
        sys.exit(3)  # distinguish idle shutdown from a startup or runtime error


if __name__ == "__main__":
    main()
