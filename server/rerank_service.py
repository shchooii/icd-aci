#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Re-ranker service for the live demo.

Scores (query, document) pairs with Qwen3-Reranker exactly as the experiments did:
the model's own yes/no judgement at the last position, softmax over the two tokens,
no generated text, no truncation. The demo server sends (note, official code title)
and (extracted query, official code title) pairs and ranks the codes by the score.

Needs torch and transformers (the environment the experiments ran in) and one GPU.
Texts are never written to a file or a log line.

Settings (environment):
  ICD_DEMO_RERANKER_PATH   folder of the model snapshot (required)
  ICD_DEMO_RERANK_HOST     default 127.0.0.1
  ICD_DEMO_RERANK_PORT     default 8766
  ICD_DEMO_RERANK_API_KEY  optional; when set, requests need "Authorization: Bearer <key>"
  ICD_DEMO_RERANK_MAX_TOKENS  longest pair in tokens, default 8192
  ICD_DEMO_RERANK_BATCH_TOKENS  tokens per forward pass, default 16000 (16 pairs of a 1,000-token note)
  ICD_DEMO_RERANK_DEVICE   default cuda:0 (choose the GPU with CUDA_VISIBLE_DEVICES)
"""

import hmac
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# The task wording and the template are those of the experiments.
QUERY_TASK = "Given a clinical text passage, retrieve ICD code descriptions matching the documented diagnoses and procedures."
RERANK_PREFIX = ('<|im_start|>system\nJudge whether the Document meets the requirements based on the Query and the Instruct '
                 'provided. Note that the answer can only be "yes" or "no".<|im_end|>\n<|im_start|>user\n')
RERANK_SUFFIX = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"


def env(name, default):
    value = os.environ.get(name)
    return default if value is None or value.strip() == "" else value.strip()


MODEL_PATH = env("ICD_DEMO_RERANKER_PATH", "")
HOST = env("ICD_DEMO_RERANK_HOST", "127.0.0.1")
PORT = int(env("ICD_DEMO_RERANK_PORT", "8766"))
API_KEY = env("ICD_DEMO_RERANK_API_KEY", "")
MAX_TOKENS = int(env("ICD_DEMO_RERANK_MAX_TOKENS", "8192"))
MAX_BATCH = int(env("ICD_DEMO_RERANK_MAX_BATCH", "64"))
BATCH_TOKENS = int(env("ICD_DEMO_RERANK_BATCH_TOKENS", "16000"))
DEVICE = env("ICD_DEMO_RERANK_DEVICE", "cuda:0")
MAX_PAIRS = int(env("ICD_DEMO_RERANK_MAX_PAIRS", "8000"))
MAX_BODY_BYTES = 64 * 1024 * 1024


def log(*parts):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), *parts, flush=True)


class TooLong(ValueError):
    pass


class Reranker(object):
    def __init__(self):
        self.ready = False
        self.error = None
        self.lock = threading.Lock()  # one scoring call at a time on the GPU

    def load(self):
        try:
            import torch
            from transformers import AutoModelForCausalLM, AutoTokenizer
            self.torch = torch
            self.tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, local_files_only=True, trust_remote_code=False,
                                                           padding_side="left")
            self.prefix_ids = self.tokenizer.encode(RERANK_PREFIX, add_special_tokens=False)
            self.suffix_ids = self.tokenizer.encode(RERANK_SUFFIX, add_special_tokens=False)
            self.no_id = self.tokenizer.convert_tokens_to_ids("no")
            self.yes_id = self.tokenizer.convert_tokens_to_ids("yes")
            if (self.no_id is None or self.yes_id is None or self.no_id == self.yes_id
                    or self.tokenizer.encode("no", add_special_tokens=False) != [self.no_id]
                    or self.tokenizer.encode("yes", add_special_tokens=False) != [self.yes_id]):
                raise ValueError("the tokenizer has no single yes/no tokens")
            started = time.time()
            self.model = AutoModelForCausalLM.from_pretrained(MODEL_PATH, local_files_only=True, trust_remote_code=False,
                                                              torch_dtype=torch.bfloat16, attn_implementation="sdpa")
            self.model = self.model.to(DEVICE).eval()
            self.model.requires_grad_(False)
            self.model.config.use_cache = False
            if MAX_TOKENS > self.model.config.max_position_embeddings:
                raise ValueError("ICD_DEMO_RERANK_MAX_TOKENS is larger than the model allows")
            self.ready = True
            log("model loaded in %.0f s on %s" % (time.time() - started, DEVICE))
        except Exception as exc:  # report the kind of failure, never any text
            self.error = type(exc).__name__ + ": " + str(exc)[:200]
            log("model failed to load:", self.error)

    def score(self, pairs):
        torch = self.torch
        prepared = []
        for query, document in pairs:
            body = "<Instruct>: %s\n<Query>: %s\n<Document>: %s" % (QUERY_TASK, query, document)
            ids = self.prefix_ids + self.tokenizer.encode(body, add_special_tokens=False) + self.suffix_ids
            if len(ids) > MAX_TOKENS:
                raise TooLong()
            prepared.append(ids)
        result = [None] * len(prepared)
        order = sorted(range(len(prepared)), key=lambda i: (len(prepared[i]), i))
        # Batches hold about BATCH_TOKENS tokens: 16 pairs of a 1,000-token note as in the experiments,
        # fewer for a longer note, and up to MAX_BATCH short pairs (query against title).
        batches, current = [], []
        for i in order:
            longest = len(prepared[i])  # sorted by length, so the newest pair is the longest of its batch
            if current and (len(current) >= MAX_BATCH or (len(current) + 1) * longest > BATCH_TOKENS):
                batches.append(current)
                current = []
            current.append(i)
        if current:
            batches.append(current)
        with self.lock:
            for indices in batches:
                batch = self.tokenizer.pad({"input_ids": [prepared[i] for i in indices],
                                            "attention_mask": [[1] * len(prepared[i]) for i in indices]},
                                           padding=True, return_tensors="pt")
                batch = {key: value.to(DEVICE) for key, value in batch.items()}
                if not bool(torch.all(batch["attention_mask"][:, -1] == 1)):
                    raise ValueError("the last position points into padding")
                with torch.inference_mode():
                    logits = self.model(**batch, logits_to_keep=1, use_cache=False).logits[:, -1, :]
                    scores = torch.softmax(logits[:, [self.no_id, self.yes_id]].float(), dim=1)[:, 1]
                if not bool(torch.isfinite(scores).all()):
                    raise ValueError("a score is not finite")
                for i, value in zip(indices, scores.cpu().tolist()):
                    result[i] = value
                del batch, logits, scores
        return result


RERANKER = Reranker()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "icd-rerank"
    sys_version = ""
    timeout = 60

    def log_message(self, fmt, *args):
        pass

    def log_error(self, fmt, *args):
        pass

    def reply(self, status, payload, route):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
        log(self.command, route, status)

    def allowed(self):
        if not API_KEY:
            return True
        given = self.headers.get("Authorization", "")
        return hmac.compare_digest(given.encode("utf-8"), ("Bearer " + API_KEY).encode("utf-8"))

    def do_GET(self):
        if self.path != "/health":
            self.reply(404, {"error": "not found"}, "other")
        elif not self.allowed():
            self.reply(401, {"error": "unauthorized"}, "/health")
        else:
            self.reply(200, {"ready": RERANKER.ready, "failed": RERANKER.error is not None,
                             "model": os.path.basename(MODEL_PATH.rstrip("/")), "max_tokens": MAX_TOKENS}, "/health")

    def do_POST(self):
        route = "/score"
        self.close_connection = True
        if self.path != route:
            self.reply(404, {"error": "not found"}, "other")
            return
        if not self.allowed():
            self.reply(401, {"error": "unauthorized"}, route)
            return
        if not RERANKER.ready:
            self.reply(503, {"error": "model not ready"}, route)
            return
        try:
            length = int(self.headers.get("Content-Length", ""))
            if length > MAX_BODY_BYTES:
                raise ValueError
            pairs = json.loads(self.rfile.read(length).decode("utf-8"))["pairs"]
            if (not isinstance(pairs, list) or len(pairs) > MAX_PAIRS
                    or any(not isinstance(p, list) or len(p) != 2 or not all(isinstance(t, str) and t for t in p) for p in pairs)):
                raise ValueError
        except (ValueError, KeyError, TypeError, OSError):
            self.reply(400, {"error": "body must be {\"pairs\": [[query, document], ...]}"}, route)
            return
        started = time.time()
        try:
            scores = RERANKER.score(pairs) if pairs else []
        except TooLong:
            self.reply(413, {"error": "a pair is longer than the token limit", "max_tokens": MAX_TOKENS}, route)
            return
        except Exception as exc:
            log("scoring failed:", type(exc).__name__)
            self.reply(500, {"error": "scoring failed"}, route)
            return
        log("scored", len(pairs), "pairs in %.1f s" % (time.time() - started))
        self.reply(200, {"scores": scores}, route)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    if not MODEL_PATH or not os.path.isdir(MODEL_PATH):
        sys.exit("ICD_DEMO_RERANKER_PATH must be the folder of the re-ranker model snapshot.")
    httpd = Server((HOST, PORT), Handler)
    log("listening on http://%s:%d (model loading)" % (HOST, PORT))
    threading.Thread(target=RERANKER.load, name="load", daemon=True).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        log("stopped")


if __name__ == "__main__":
    main()
