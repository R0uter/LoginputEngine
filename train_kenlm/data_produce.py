"""
Phase 2: Process corpus using the Unigram tokenizer word list for segmentation.

For each line:
  1. Convert traditional → simplified Chinese (OpenCC)
  2. Replace punctuation with sentence-boundary markers
  3. Tokenize each Chinese chunk using the trained Unigram tokenizer
     (uses the tokenizer's Viterbi decoder, not naive max-match)
  4. Simultaneously derive per-token pinyin from sentence-level pypinyin
     (sentence context gives better polyphonic character handling)

Outputs:
  result_files/data_cuted.txt   — space-separated tokens, one sentence per line
                                  (ready for KenLM lmplz)
  result_files/word_pinyin.txt  — word TAB pinyin mapping for emission DB
"""

import gc
import os
import re
import signal
import sys
import time
import datetime
import multiprocessing
import string

from zhon import hanzi
import tqdm
from pypinyin import Style, pinyin as _pypinyin

import utility

ARTICLE_DIR = './articles'
FILEDIR = './result_files'
DATA_TMP = './result_files/data_tmp'
WORD_PINYIN_TMP = './result_files/word_pinyin_tmp'
DATA_TXT_FILE = './result_files/data_cuted.txt'
WORD_PINYIN_FILE = './result_files/word_pinyin.txt'
TOKENIZER_PATH = './result_files/tokenizer/tokenizer.json'

kGB18030 = 'gb18030'
kEndProcess = '-=-=-=-EOF=-=-=-=-'

last_time_flush_check = datetime.datetime.now()
PROCESS_NUM = 5
MEMORY_LIMIT_GB = 20 / PROCESS_NUM

# Replace all punctuation (Chinese + printable ASCII + fullwidth space) with '_'
ALLPUNC = '[{}{}　]'.format(hanzi.punctuation, string.printable)
# Match sequences of CJK Unified Ideographs (Basic + Extension A)
_CJK_SEQ = re.compile(r'[\u4e00-\u9fff\u3400-\u4dbf]+')

# ── Per-worker state (each subprocess gets its own copy via fork) ─────────────
lines_cache = []          # KenLM corpus lines waiting to be flushed to disk
word_pinyin_cache = {}    # word → pinyin  (accumulated; written once at final flush)
jobs = []
queue = multiprocessing.Queue(500)
current_idx = 0
pbar = None

# Lazy-loaded tokenizer (initialised once per worker process)
_tokenizer = None


def _get_tokenizer():
    """Return the Unigram tokenizer, loading it on first call in this process."""
    global _tokenizer
    if _tokenizer is None:
        from tokenizers import Tokenizer as _Tokenizer
        _tokenizer = _Tokenizer.from_file(TOKENIZER_PATH)
    return _tokenizer


# ── Flush helpers ─────────────────────────────────────────────────────────────

def flush_if_needed(force: bool = False):
    """Flush lines_cache to disk when memory is tight or force=True.
    word_pinyin_cache is only written to disk on the final force flush to avoid
    duplicate entries (the cache is bounded by tokenizer vocab size, ~200K entries,
    so keeping it in memory throughout is fine).
    """
    global lines_cache, last_time_flush_check

    if not force:
        # Only check memory every 10 minutes to avoid overhead
        if (datetime.datetime.now() - last_time_flush_check).seconds <= (10 * 60):
            return
        last_time_flush_check = datetime.datetime.now()
        if utility.get_current_memory_gb() < MEMORY_LIMIT_GB:
            return

    pid = str(os.getpid())
    memory_alloc = utility.get_current_memory_gb()
    print('|---Current memory alloc: ', int(memory_alloc))
    print('|---Needs flush to disk: ', memory_alloc >= MEMORY_LIMIT_GB, 'Force to: ', force)
    print('|---🚽 Flushing...')

    # ── Flush KenLM corpus chunk ──────────────────────────────────────────────
    data_path = DATA_TMP + '-' + pid
    with open(data_path, 'a', encoding=kGB18030) as f:
        f.writelines(lines_cache)
    lines_cache.clear()

    # ── Flush word→pinyin map (only on final flush) ───────────────────────────
    if force:
        pinyin_path = WORD_PINYIN_TMP + '-' + pid
        with open(pinyin_path, 'w', encoding='utf8') as f:
            for word, py in word_pinyin_cache.items():
                f.write(f'{word}\t{py}\n')
        print(f'|---📝 Word-pinyin pairs written: {len(word_pinyin_cache):,}')

    gc.collect()
    print('|---🧻 Done, now memory alloc: ', int(utility.get_current_memory_gb()))


# ── Worker process ────────────────────────────────────────────────────────────

def sub_processing_signal_handler(sig, frame):
    print('|---Sub process received SIGINT, finalizing...')


def processing_line(q: multiprocessing.Queue, process_num: int = 10, mem_limit_gb: int = 10):
    """Worker process: consume lines from the queue and call sub_process_line."""
    global PROCESS_NUM, MEMORY_LIMIT_GB
    signal.signal(signal.SIGINT, sub_processing_signal_handler)
    PROCESS_NUM = process_num
    MEMORY_LIMIT_GB = mem_limit_gb / PROCESS_NUM

    _get_tokenizer()  # warm up so first line doesn't pay the load cost
    print('|---Worker process ready...')

    while True:
        if q.empty():
            time.sleep(0.1)
            continue
        s = q.get()
        if s == kEndProcess:
            print('|---Finish and flushing...')
            q.put(kEndProcess)
            flush_if_needed(force=True)
            break
        try:
            sub_process_line(s)
        except Exception as e:
            print('Error in subprocess: ', e)


def _tokenize_chunk(cjk_text: str):
    """Tokenize a pure-CJK string with the Unigram tokenizer.

    Uses character offsets from the encoding to reconstruct token strings from
    the original text — this is robust against any marker prefixes the tokenizer
    may add (e.g. '▁') and guarantees tokens are exact substrings of cjk_text.

    Returns:
        list[str]: Token strings whose concatenation equals cjk_text.
                   Falls back to individual characters if reconstruction fails.
    """
    tok = _get_tokenizer()
    encoding = tok.encode(cjk_text)

    # Reconstruct tokens from character offsets (robust against special markers)
    tokens = [cjk_text[s:e] for s, e in encoding.offsets
              if cjk_text[s:e] and cjk_text[s:e] != '<unk>']

    # Sanity check: tokens must cover the full input exactly
    if ''.join(tokens) != cjk_text:
        # Fall back to character-level segmentation
        tokens = list(cjk_text)

    return tokens


def _get_token_pinyins(cjk_text: str, tokens: list) -> dict:
    """Derive pinyin for each token using sentence-level pypinyin.

    Running pypinyin on the full sentence (not on individual words) gives better
    accuracy for polyphonic characters because the model uses surrounding context.

    Returns:
        dict[str, str]: token → pinyin string (syllables joined with "'")
    """
    py_result = _pypinyin(cjk_text, style=Style.NORMAL, strict=False)
    per_char_py = [r[0] for r in py_result]

    token_pinyins = {}
    char_idx = 0
    for token in tokens:
        token_len = len(token)
        py = "'".join(per_char_py[char_idx: char_idx + token_len])
        if token not in token_pinyins:
            token_pinyins[token] = py
        char_idx += token_len

    return token_pinyins


def sub_process_line(s: str):
    """Process one raw corpus line:
      - t2s convert → replace punctuation → extract CJK chunks
      - tokenize each chunk with Unigram tokenizer
      - derive per-token pinyin from sentence context
      - cache results for flushing
    """
    flush_if_needed()

    line = utility.t2s(s)
    line = re.sub(ALLPUNC, '_', line)

    # Each chunk between '_' delimiters becomes one sentence in data_cuted.txt
    chunk_token_lists = []
    for chunk in line.split('_'):
        chunk = chunk.strip()
        if len(chunk) < 2:
            continue

        # Extract only CJK sequences (filter leftover non-CJK unicode characters)
        cjk_matches = _CJK_SEQ.findall(chunk)
        chunk_tokens = []
        for cjk_text in cjk_matches:
            if len(cjk_text) < 1:
                continue
            tokens = _tokenize_chunk(cjk_text)
            token_pinyins = _get_token_pinyins(cjk_text, tokens)
            chunk_tokens.extend(tokens)
            # Record pinyin (first occurrence per word wins)
            for word, py in token_pinyins.items():
                if word not in word_pinyin_cache:
                    word_pinyin_cache[word] = py

        if chunk_tokens:
            chunk_token_lists.append(' '.join(chunk_tokens))

    if chunk_token_lists:
        # Join chunks with '_' so merge_tmp_files can split them into separate
        # sentences (preserving the same convention as the original pipeline)
        lines_cache.append('_'.join(chunk_token_lists) + '\n')


# ── File management ───────────────────────────────────────────────────────────

def remove_tmp_files():
    """Remove all data_tmp-* and word_pinyin_tmp-* files from FILEDIR."""
    for root, _, filenames in os.walk(FILEDIR):
        for filename in filenames:
            p = os.path.join(root, filename)
            if 'data_tmp-' in filename or 'word_pinyin_tmp-' in filename:
                os.remove(p)


def merge_tmp_files():
    """Merge per-worker tmp files into the final output files."""
    # ── KenLM corpus ─────────────────────────────────────────────────────────
    with open(DATA_TXT_FILE, mode='a', encoding='utf8') as f_out:
        for root, _, filenames in os.walk(FILEDIR):
            for filename in sorted(filenames):
                p = os.path.join(root, filename)
                if 'data_tmp-' not in filename:
                    continue
                with open(p, 'r', encoding=kGB18030, errors='ignore') as t:
                    for line in t:
                        for sub_line in line.split('_'):
                            sub_line = sub_line.strip()
                            if len(sub_line) <= 1:
                                continue
                            f_out.write(sub_line + '\n')

    # ── Word → pinyin mapping ─────────────────────────────────────────────────
    merged_pinyin: dict = {}
    for root, _, filenames in os.walk(FILEDIR):
        for filename in sorted(filenames):
            p = os.path.join(root, filename)
            if 'word_pinyin_tmp-' not in filename:
                continue
            with open(p, 'r', encoding='utf8') as f:
                for line in f:
                    line = line.strip()
                    if '\t' not in line:
                        continue
                    word, py = line.split('\t', 1)
                    merged_pinyin.setdefault(word, py)  # first seen wins

    with open(WORD_PINYIN_FILE, 'w', encoding='utf8') as f_out:
        for word, py in sorted(merged_pinyin.items()):
            f_out.write(f'{word}\t{py}\n')

    print(f'📖 Word-pinyin file: {len(merged_pinyin):,} entries → {WORD_PINYIN_FILE}')
    remove_tmp_files()


# ── Orchestration ─────────────────────────────────────────────────────────────

def end_and_exit():
    global pbar
    pbar.close()
    queue.put(kEndProcess)
    print('Waiting subprocess to exit')
    for p in jobs:
        while p.is_alive():
            print('Queue is not empty yet, check again after 3s...')
            time.sleep(3)
    print('Merging tmp files...')
    merge_tmp_files()


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    print('Subprocess still needs to process the rest of the queue, please wait...')
    end_and_exit()
    print('\n\nCurrent index number: ', current_idx)
    sys.exit(0)


def gen_data_txt(process_num: int = 10, mem_limit_gb: int = 10):
    """Phase 2 entry point: segment corpus and generate word-pinyin mapping.

    Requires:
      result_files/tokenizer/tokenizer.json  — produced by build_tokenizer.gen_word_list()

    Args:
        process_num:  Number of worker processes (recommend = CPU core count).
        mem_limit_gb: Total memory budget in GB shared across all workers.
    """
    global current_idx
    current_idx = 0
    start_line = 0

    signal.signal(signal.SIGINT, signal_handler)
    print('💭 Start processing corpus (Phase 2)...')

    all_files = []
    total_bytes = 0
    for root, _, filenames in os.walk(ARTICLE_DIR):
        for filename in filenames:
            p = os.path.join(root, filename)
            if p.endswith('.txt'):
                all_files.append(p)
                total_bytes += utility.read_bytes_from(p)
    all_files = sorted(all_files)

    print(f'''
        |--- Files:      {len(all_files)}
        |--- Total size: {round(total_bytes / 1024 / 1024 / 1024, 2)} GB
    ''')

    remove_tmp_files()

    for _ in range(process_num):
        p = multiprocessing.Process(
            target=processing_line,
            args=(queue, process_num, mem_limit_gb),
        )
        jobs.append(p)
        p.start()

    global pbar
    pbar = tqdm.tqdm(total=total_bytes, unit='B', unit_scale=True, unit_divisor=1024)

    for path in all_files:
        print('Processing file: ', path)
        # Detect encoding: try utf8 first (stricter), then gb18030.
        detected_enc = None
        for enc in ('utf8', kGB18030):
            try:
                with open(path, 'r', encoding=enc) as probe:
                    probe.read(1024 * 1024)  # read up to 1M chars to verify
                detected_enc = enc
                break
            except Exception:
                pass

        if detected_enc is None:
            pbar.update(utility.read_bytes_from(path))
            print(f'Wrong encoding of file {path}, skip...')
            continue

        f = open(path, 'r', encoding=detected_enc, errors='ignore')
        for line in f:
            current_idx += 1
            pbar.update(len(line.encode(detected_enc, errors='ignore')))
            if current_idx < start_line:
                continue
            while queue.full():
                time.sleep(0.1)
            queue.put(line)
        f.close()

    end_and_exit()
    print('Corpus processing finished.')
    print(f'  KenLM corpus: {DATA_TXT_FILE}')
    print(f'  Word-pinyin:  {WORD_PINYIN_FILE}')
