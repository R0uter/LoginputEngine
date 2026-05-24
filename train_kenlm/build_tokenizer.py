"""
Phase 1: Train a HuggingFace Unigram tokenizer on the raw corpus and export
a Chinese-only word list for use in Phase 2 corpus processing (data_produce.py).

Outputs:
  result_files/tokenizer/tokenizer.json  — reusable trained tokenizer
  result_files/word_list.txt             — one Chinese word per line
"""

import gc
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
MAX_WORD_LENGTH = 8
DEFAULT_VOCAB_SIZE = 200_000

# Match tokens that are purely CJK Unified Ideographs (Basic + Extension A)
_CHINESE_ONLY_RE = re.compile(r'^[\u4e00-\u9fff\u3400-\u4dbf]+$')
_cc = OpenCC('t2s')


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

    with open(output_path, 'w', encoding='utf8') as out:
        for path in all_files:
            f, enc = _open_corpus_file(path)
            if f is None:
                print(f'⚠️  Cannot read {path}, skipping.')
                pbar.update(os.path.getsize(path))
                continue
            for line in f:
                pbar.update(len(line.encode(enc, errors='ignore')))
                line = line.strip()
                if line:
                    out.write(_cc.convert(line) + '\n')
            f.close()

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
    tokenizer.train([TMP_CORPUS_FILE], trainer)
    tokenizer.save(TOKENIZER_PATH)
    print(f'    💾 Tokenizer saved to {TOKENIZER_PATH}')

    # Clean up temporary corpus
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
