import utility
from dag.priorityset import PrioritySet
import lmdb
import struct
import sqlite3
import math
import res.pinyin_data

kLMDB = 'LMDB'
kSQLITE = 'SQLITE'
kRAWDATA = 'RAWDATA'
kGB18030 = 'gb18030'
Database_Type = kLMDB

LMDB_FILE = './result_files/transition.mdb'
PY_DATABASE = './result_files/emission.db'
SQLITE_DIR = './result_files/db.sqlite'
GRAM1FILE = './result_files/1gram_transition.json'
GRAM2FILE = './result_files/2gram_transition.json'
GRAM3FILE = './result_files/3gram_transition.json'
PY2WORDSFILE = './result_files/pinyin2words.json'

print('Loading data.')
if Database_Type == kRAWDATA:
    py2words_data = utility.readjsondatafromfile(PY2WORDSFILE)
    gram1data = utility.readjsondatafromfile(GRAM1FILE)
    gram2data = utility.readjsondatafromfile(GRAM2FILE)
    gram3data = utility.readjsondatafromfile(GRAM3FILE)

if Database_Type == kLMDB:
    env = lmdb.open(LMDB_FILE,
                    map_size=1048576000,
                    readonly=True,
                    lock=False,
                    meminit=False,
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

if Database_Type == kSQLITE:
    con = sqlite3.connect(SQLITE_DIR)
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
print('Done.')


def _get_words_from(pinyin: [str]) -> [str]:
    pys = "'".join(pinyin)
    if Database_Type == kRAWDATA:
        if pys not in py2words_data: return None
        return py2words_data[pys]
    else:
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
    if Database_Type == kRAWDATA:
        if word in gram1data:
            return gram1data[word]
        return gram1data['min_value']
    if Database_Type == kLMDB:
        with env.begin(write=False) as t:
            data = t.get(word.encode(kGB18030))
            if not data:
                data = t.get('min_value'.encode(kGB18030))
        return struct.unpack('d', data)[0]

    if Database_Type == kSQLITE:
        c = con.execute(
            'select weight from transition where w1="-" and w2="-" and w3 in (?,?)',
            [word, 'min_value'])
        w = -math.inf
        for row in c:
            w = max(row[0], w)
        return w


def _get_words_with_gram_1_weight_from(pinyin: [str]) -> {}:
    result = {}
    words = _get_words_from(pinyin)
    if not words: return None
    for word in words:
        weight = _get_gram_1_weight_from(word)
        result[word] = weight
    return result


def _get_gram_2_weight_from(last_one: str, one: str) -> float:
    if Database_Type == kRAWDATA:
        if one not in gram2data:
            return gram1data['min_value']
        if last_one not in gram2data[one]:
            return gram1data[one]
        return gram2data[one][last_one]

    if Database_Type == kLMDB:
        key = '{}_{}'.format(last_one, one).encode(kGB18030)
        with env.begin() as t:
            data = t.get(key)
        if data:
            return struct.unpack('d', data)[0]
        return _get_gram_1_weight_from(one)

    if Database_Type == kSQLITE:
        c = con.execute(
            'select w2,w3,weight from transition where w1=? and w2 in (?,?) and w3 in (?,?)',
            ['-', '-', last_one, one, 'min_value'])
        w1, w2, w3 = (0., 0., 0.)
        for row in c:
            if row[0] == last_one:
                w1 = row[2]
                break
            if row[1] == one:
                w2 = row[2]
                continue
            if row[1] == 'min_value':
                w3 = row[2]
        if w1 != 0: return w1
        if w2 != 0: return w2
        return w3


def _get_gram_3_weight_from(last_last_one: str, last_one: str,
                            one: str) -> float:
    if Database_Type == kRAWDATA:
        if one in gram3data:
            if last_one in gram3data[one]:
                if last_last_one not in gram3data[one][last_one]:
                    return _get_gram_2_weight_from(last_one, one)

                return gram3data[one][last_one][last_last_one]

        return _get_gram_1_weight_from(one)

    if Database_Type == kLMDB:
        key = '{}_{}_{}'.format(last_last_one, last_one, one).encode(kGB18030)
        # key2 = '{}_{}'.format(last_one, one).encode(kGB18030)
        # key3 = one.encode(kGB18030)
        with env.begin() as t:
            data = t.get(key)
        if data:
            return struct.unpack('d', data)[0]
        return _get_gram_2_weight_from(last_one, one)

    if Database_Type == kSQLITE:
        c = con.execute(
            'select w1,w2,w3,weight from transition where w1 in (?,?) and w2 in (?,?) and w3 in (?,?)',
            ['-', last_last_one, '-', last_one, one, 'min_value'])
        w1, w2, w3, w4 = (0., 0., 0., 0.)
        for row in c:
            if row[0] == last_last_one:
                w1 = row[3]
                break
            if row[1] == last_one:
                w2 = row[3]
                continue
            if row[2] == one:
                w3 = row[3]
                continue
            if row[2] == 'min_value':
                w4 = row[3]
        if w1 != 0: return w1
        if w2 != 0: return w2
        if w3 != 0: return w3
        return w4


def get_candidates_from(py: str, path_num=6, log=False) -> list:
    pinyin_list = py.split("'")
    pinyin_num = len(pinyin_list)
    if pinyin_num == 0: return []
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
                cut = pinyin_list[from_index:to_idx + 1]
                words = _get_words_from(cut)
                if words is None: continue
                for prev_item in prev_paths:
                    last_one = prev_item.path[0]
                    for word in words:
                        new_path = prev_item.path + [word]
                        new_score = _get_gram_2_weight_from(last_one, word)
                        if log:
                            score = prev_item.score + new_score
                        else:
                            score = prev_item.score * new_score
                        Graph[to_idx].put(score, new_path)

    # 第三个字词往后处理 gram3
    if pinyin_num >= 3:
        for last_index, prev_paths in enumerate(Graph):
            from_index = last_index + 1
            for to_idx in range(from_index, pinyin_num):
                cut = pinyin_list[from_index:to_idx + 1]
                words = _get_words_from(cut)
                if words is None: continue
                for prev_item in prev_paths:
                    if len(prev_item.path) < 2: continue
                    last_one = prev_item.path[-1]
                    last_last_one = prev_item.path[-2]
                    for word in words:
                        new_path = prev_item.path + [word]
                        new_score = _get_gram_3_weight_from(
                            last_last_one, last_one, word)
                        if log:
                            score = prev_item.score + new_score
                        else:
                            score = prev_item.score * new_score
                        Graph[to_idx].put(score, new_path)

    result = [item for item in Graph[-1]]
    return sorted(result, key=lambda item: item.score, reverse=True)