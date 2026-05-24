"""
Phase 1: Train a HuggingFace Unigram tokenizer on the raw corpus and export
a Chinese-only word list for use in Phase 2 corpus processing (data_produce.py).

Outputs:
  result_files/tokenizer/tokenizer.json  — reusable trained tokenizer
  result_files/word_list.txt             — one Chinese word per line
"""

import gc
import itertools
import multiprocessing
import os
import re

import tqdm
from opencc import OpenCC
from tokenizers import Tokenizer
from tokenizers.models import Unigram
from tokenizers.pre_tokenizers import UnicodeScripts
from tokenizers.trainers import UnigramTrainer

ARTICLE_DIR = './articles'
FILEDIR = './result_files'
TOKENIZER_DIR = './result_files/tokenizer'
TOKENIZER_PATH = './result_files/tokenizer/tokenizer.json'
WORD_LIST_FILE = './result_files/word_list.txt'
TMP_CORPUS_FILE = './result_files/tokenizer_corpus_tmp.txt'

kGB18030 = 'gb18030'
MAX_WORD_LENGTH = 6
DEFAULT_VOCAB_SIZE = 200_000

# Match tokens that are purely CJK Unified Ideographs (Basic + Extension A)
_CHINESE_ONLY_RE = re.compile(r'^[\u4e00-\u9fff\u3400-\u4dbf]+$')

_worker_cc = None

def _init_worker():
    global _worker_cc
    _worker_cc = OpenCC('t2s')

def _process_chunk(args):
    chunk, chunk_bytes = args
    return [_worker_cc.convert(line) for line in chunk], chunk_bytes

def _chunk_generator(f, enc, chunk_size=10000):
    chunk = []
    chunk_bytes = 0
    for line in f:
        chunk_bytes += len(line.encode(enc, errors='ignore'))
        line = line.strip()
        if line:
            chunk.append(line)
            
        if len(chunk) >= chunk_size:
            yield chunk, chunk_bytes
            chunk = []
            chunk_bytes = 0
            
    if chunk or chunk_bytes > 0:
        yield chunk, chunk_bytes


def _open_corpus_file(path: str):
    """Try to open a corpus file with gb18030 then utf8 encoding.
    Returns (file_object, encoding) or (None, None) if both fail."""
    for enc in (kGB18030, 'utf8'):
        try:
            f = open(path, encoding=enc)
            f.readline()  # probe
            f.seek(0)
            return f, enc
        except Exception:
            try:
                f.close()
            except Exception:
                pass
    return None, None


def _collect_corpus(output_path: str):
    """Read all corpus articles, convert to simplified Chinese, and write to
    a single UTF-8 file for the tokenizer trainer."""
    all_files = []
    total_bytes = 0
    for root, _, filenames in os.walk(ARTICLE_DIR):
        for filename in sorted(filenames):
            if filename.endswith('.txt'):
                p = os.path.join(root, filename)
                all_files.append(p)
                total_bytes += os.path.getsize(p)
    all_files.sort()

    print(f'    |--- Files: {len(all_files)}, Total: {round(total_bytes / 1024 ** 3, 2)} GB')
    pbar = tqdm.tqdm(total=total_bytes, unit='B', unit_scale=True, unit_divisor=1024)

    num_processes = max(1, (os.cpu_count() or 2) - 1)
    pool = multiprocessing.Pool(processes=num_processes, initializer=_init_worker)

    with open(output_path, 'w', encoding='utf8') as out:
        for path in all_files:
            f, enc = _open_corpus_file(path)
            if f is None:
                print(f'⚠️  Cannot read {path}, skipping.')
                pbar.update(os.path.getsize(path))
                continue
            
            gen = _chunk_generator(f, enc, chunk_size=10000)
            while True:
                batch = list(itertools.islice(gen, 100))
                if not batch:
                    break
                for result_chunk, chunk_bytes in pool.imap(_process_chunk, batch):
                    pbar.update(chunk_bytes)
                    for converted_line in result_chunk:
                        out.write(converted_line + '\n')
            f.close()

    pool.close()
    pool.join()
    pbar.close()
    gc.collect()
    print(f'    ✅ Corpus written to {output_path}')


def gen_word_list(vocab_size: int = DEFAULT_VOCAB_SIZE) -> list:
    """Train a Unigram tokenizer on the full corpus and export a Chinese word list.

    Args:
        vocab_size: Target vocabulary size for the tokenizer. Larger values
            produce more word-level tokens; smaller values produce more
            character-level fallbacks. Default: 200,000.

    Returns:
        Sorted list of Chinese words written to WORD_LIST_FILE.
    """
    os.makedirs(TOKENIZER_DIR, exist_ok=True)

    # ── Step 1: Collect corpus ────────────────────────────────────────────────
    if os.path.exists(TMP_CORPUS_FILE):
        print(f'📝 Step 1/3: Using existing temporary corpus at {TMP_CORPUS_FILE}')
    else:
        print('📝 Step 1/3: Collecting and converting corpus...')
        _collect_corpus(TMP_CORPUS_FILE)

    # ── Step 2: Train Unigram tokenizer ──────────────────────────────────────
    print(f'🔧 Step 2/3: Training Unigram tokenizer (vocab_size={vocab_size:,})...')
    tokenizer = Tokenizer(Unigram())
    # UnicodeScripts splits at script boundaries (CJK / Latin / etc.) so the
    # Unigram model can learn multi-character Chinese tokens within each chunk.
    tokenizer.pre_tokenizer = UnicodeScripts()

    trainer = UnigramTrainer(
        vocab_size=vocab_size,
        special_tokens=['<unk>'],
        unk_token='<unk>',
        max_piece_length=MAX_WORD_LENGTH,  # prevents long phrases from being learned as tokens
    )
    
    def corpus_iterator(file_path, chunk_size=10000, max_lines=5_000_000):
        """Yield chunks of text to avoid loading 20GB into RAM at once.
        Caps at max_lines (e.g. 5M) so Unigram doesn't consume all 32GB memory."""
        with open(file_path, 'r', encoding='utf8') as f:
            batch = []
            lines_read = 0
            for line in f:
                batch.append(line.strip())
                lines_read += 1
                if len(batch) >= chunk_size:
                    yield batch
                    batch = []
                    if lines_read >= max_lines:
                        break
            if batch:
                yield batch

    # Train from iterator instead of loading the whole file list directly
    tokenizer.train_from_iterator(corpus_iterator(TMP_CORPUS_FILE), trainer)
    tokenizer.save(TOKENIZER_PATH)
    print(f'    💾 Tokenizer saved to {TOKENIZER_PATH}')

    os.remove(TMP_CORPUS_FILE)

    # ── Step 3: Export word list ──────────────────────────────────────────────
    print('📋 Step 3/3: Exporting Chinese word list...')
    vocab = tokenizer.get_vocab()
    words = sorted(
        token for token in vocab
        if _CHINESE_ONLY_RE.match(token) and 1 <= len(token) <= MAX_WORD_LENGTH
    )
    with open(WORD_LIST_FILE, 'w', encoding='utf8') as f:
        for word in words:
            f.write(word + '\n')

    print(f'    ✅ Word list: {len(words):,} words → {WORD_LIST_FILE}')
    return words


def load_tokenizer() -> Tokenizer:
    """Load the saved tokenizer from disk."""
    return Tokenizer.from_file(TOKENIZER_PATH)


def load_word_list() -> frozenset:
    """Load the word list from disk as a frozenset for O(1) membership lookup."""
    with open(WORD_LIST_FILE, 'r', encoding='utf8') as f:
        return frozenset(line.strip() for line in f if line.strip())
