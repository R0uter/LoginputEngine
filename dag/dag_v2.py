import utility
from dag.priorityset import PrioritySet
import lmdb
import struct
import sqlite3
import math
import res.pinyin_data

kGB18030 = 'gb18030'
LMDB_FILE = './result_files/transition_v2.mdb'
PY_DATABASE = './result_files/emission_v2.db'

env: lmdb.Environment = None
con = None


def load_data():
    print('Loading data.')
    global env, con
    if env: env.close()
    if con: con.close()

    env = lmdb.open(LMDB_FILE,
                    map_size=1048576000,
                    readonly=True,
                    lock=False,
                    subdir=False)
    con = sqlite3.connect(PY_DATABASE)
    con.executescript('''
            PRAGMA page_size = 4096;
            PRAGMA locking_mode =  EXCLUSIVE;
            PRAGMA temp_store = MEMORY;
            PRAGMA cache_size = 40960;
            PRAGMA mmap_size = 40960;
            PRAGMA synchronous = OFF;
            PRAGMA journal_mode = OFF;
            PRAGMA query_only = 1;
            ''')
    con.commit()
    print(env.stat(), env.info())
    print('Done.')


load_data()

bow_cache = {}


def _get_words_from(pinyin: [str]) -> [str]:
    if len(pinyin) > 8: return None
    s = ''
    for i, py in enumerate(pinyin):
        pyi = res.pinyin_data.s2i_dict[py]
        s += 'p{} in ({}) and '.format(i + 1, pyi)
    s = s[:-4]
    cur = con.execute('select Words from w{} where {} '.format(
        len(pinyin), s))
    for row in cur:
        words = row[0].decode(kGB18030)
        # print(words)
        return words.split('_')


def _get_gram_1_weight_from(word: str) -> float:
    with env.begin(write=False) as t:
        data = t.get(word.encode(kGB18030))
        if not data:
            data = t.get('<unk>'.encode(kGB18030))
            return struct.unpack('<d', data[:8])[0]
    if word not in bow_cache:
        bow_cache[word] = struct.unpack('<d', data[8:])[0]
    return struct.unpack('<d', data[:8])[0]


def _get_gram_1_bow_from(word: str) -> float or None:
    if word in bow_cache:
        # print('cache hit', word)
        return bow_cache[word]

    with env.begin(write=False) as t:
        data = t.get(word.encode(kGB18030))
        if not data:
            return None
    return struct.unpack('<d', data[8:])[0]


def _get_words_with_gram_1_weight_from(pinyin: [str]) -> {}:
    result = {}
    words = _get_words_from(pinyin)
    if not words: return None
    for word in words:
        weight = _get_gram_1_weight_from(word)
        result[word] = weight
    return result


def _get_gram_2_weight_from(last_one: str, one: str) -> float:
    key = '{}_{}'.format(last_one, one).encode(kGB18030)

    with env.begin() as t:
        data = t.get(key)
        if not data:
            bow = _get_gram_1_bow_from(last_one) or 0
            return bow + _get_gram_1_weight_from(one)
    if key not in bow_cache:
        bow_cache[key] = struct.unpack('<d', data[8:])[0]
    return struct.unpack('<d', data[:8])[0]


def _get_gram_2_bow_from(last_one: str, one: str) -> float or None:
    key = '{}_{}'.format(last_one, one).encode(kGB18030)
    if key in bow_cache:
        # print('cache hit', key)
        return bow_cache[key]

    with env.begin() as t:
        data = t.get(key)
        if not data:
            return None

    return struct.unpack('<d', data[8:])[0]


def _get_gram_3_weight_from(last_last_one: str, last_one: str,
                            one: str) -> float:
    key = '{}_{}_{}'.format(last_last_one, last_one, one).encode(kGB18030)
    with env.begin() as t:
        data = t.get(key)
        if not data:
            bow = _get_gram_2_bow_from(last_last_one, last_one) or 0
            return bow + _get_gram_2_weight_from(last_one, one)
    return struct.unpack('<d', data)[0]


def get_candidates_from(py: str, path_num=6) -> list:
    pinyin_list = py.split("'")
    pinyin_num = len(pinyin_list)
    if pinyin_num == 0: return []
    cache = {}
    Graph = [PrioritySet(path_num) for _ in range(pinyin_num)]

    # 第一个词的处理
    from_index = 0
    for to_idx in range(from_index, pinyin_num):
        cut = pinyin_list[from_index:to_idx + 1]
        d = _get_words_with_gram_1_weight_from(cut)
        if d:
            words_weights = d.items()
        else:
            continue
        for word, weight in words_weights:
            Graph[to_idx].put(weight, [word])

    # 第二个字词的处理
    if pinyin_num >= 2:
        for last_index, prev_paths in enumerate(Graph):
            from_index = last_index + 1
            for to_idx in range(from_index, pinyin_num):
                key = from_index * 100 + to_idx
                if key in cache:
                    words = cache[key]
                else:
                    cut = pinyin_list[from_index:to_idx + 1]
                    words = _get_words_from(cut)
                    if words is None: continue
                    cache[key] = words

                for prev_item in prev_paths:
                    if len(prev_item.path) != 1: continue
                    last_one = prev_item.path[0]
                    for word in words:
                        new_path = prev_item.path + [word]
                        new_score = _get_gram_2_weight_from(last_one, word)
                        score = prev_item.score + new_score
                        Graph[to_idx].put(score, new_path)

    # 第三个字词往后处理 gram3
    if pinyin_num >= 3:
        for last_index, prev_paths in enumerate(Graph):
            from_index = last_index + 1
            for to_idx in range(from_index, pinyin_num):
                key = from_index * 100 + to_idx
                if key in cache:
                    words = cache[key]
                else:
                    cut = pinyin_list[from_index:to_idx + 1]
                    words = _get_words_from(cut)
                    if words is None: continue
                    cache[key] = words

                for prev_item in prev_paths:
                    if len(prev_item.path) < 2: continue
                    last_one = prev_item.path[-1]
                    last_last_one = prev_item.path[-2]
                    for word in words:
                        new_path = prev_item.path + [word]
                        new_score = _get_gram_3_weight_from(last_last_one, last_one, word)
                        score = prev_item.score + new_score
                        Graph[to_idx].put(score, new_path)

    result = [item for item in Graph[-1]]
    return sorted(result, key=lambda item: item.score, reverse=True)


def evalue(phrase: str, path_num=6, log=False) -> list:
    phrase_num = len(phrase)
    if phrase_num == 0: return []
    Graph = [PrioritySet(path_num) for _ in range(phrase_num)]

    # 第一个词的处理
    from_index = 0
    for to_idx in range(from_index, phrase_num):
        word = phrase[from_index:to_idx + 1]
        Graph[to_idx].put(_get_gram_1_weight_from(word), [word])

    # 第二个字词的处理
    if phrase_num >= 2:
        for last_index, prev_paths in enumerate(Graph):
            from_index = last_index + 1
            for to_idx in range(from_index, phrase_num):
                word = phrase[from_index:to_idx + 1]
                for prev_item in prev_paths:
                    last_one = prev_item.path[0]
                    new_path = prev_item.path + [word]
                    new_score = _get_gram_2_weight_from(last_one, word)
                    score = prev_item.score + new_score
                    Graph[to_idx].put(score, new_path)

    # 第三个字词往后处理 gram3
    if phrase_num >= 3:
        for last_index, prev_paths in enumerate(Graph):
            from_index = last_index + 1
            for to_idx in range(from_index, phrase_num):
                word = phrase[from_index:to_idx + 1]
                for prev_item in prev_paths:
                    if len(prev_item.path) < 2: continue
                    last_one = prev_item.path[-1]
                    last_last_one = prev_item.path[-2]
                    new_path = prev_item.path + [word]
                    new_score = _get_gram_3_weight_from(last_last_one, last_one, word)
                    score = prev_item.score + new_score
                    Graph[to_idx].put(score, new_path)

    result = [item for item in Graph[-1]]
    return sorted(result, key=lambda item: item.score, reverse=True)
