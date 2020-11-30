import os
import operator
import lmdb
import utility
import tqdm
import struct
import math
import sqlite3
import res.pinyin_data
import re
import multiprocessing

GRAM1FILE = './result_files/1gram_transition.json'
GRAM2FILE = './result_files/2gram_transition.json'
GRAM3FILE = './result_files/3gram_transition.json'
PY2WORDSFILE = './result_files/pinyin2words.json'

LMDB_FILE = './result_files/transition.mdb'
PY_DATABASE = './result_files/emission.db'
SQLITE_DIR = './result_files/db.sqlite'
data_to_write = {}

MAX_WORD_LENGTH = 8

def genPyTransition():
    g1 = utility.readjsondatafromfile(GRAM1FILECOUNT)
    data = {}
    pbar = tqdm.tqdm(total=len(g1))
    for word, count in g1.items():
        pbar.update()
        pylist = utility.get_pinyin_list(word)

        if not re.match('[A-z]', ''.join(pylist)):
            continue

        for i in range(0,len(pylist)):
            data.setdefault(pylist[i], 0)
            data[pylist[i]] += count

            if i+1 < len(pylist):
                k = "{}_{}".format(pylist[i], pylist[i+1])
                data.setdefault(k, 0)
                data[k] += count

    pbar.close()
    all_count = 0
    min_value = 999999999.
    max_value = 0.

    pbar = tqdm.tqdm(total=len(data))

    for word, v in data.items():
        pbar.update(0.5)
        all_count += v

    for word in list(data.keys()):
        pbar.update(0.5)
        n = data[word] / all_count
        data[word] = n
        min_value = min(n, min_value)
        max_value = max(n, max_value)

    data['min_value'] = min_value
    data['max_value'] = max_value
    pbar.close()
    utility.writejson2file(data, PY_TRANSITION_COUNT)
    utility.writePlist2File(data, "./result_files/py_transition_count.plist")


def _get_data_ready():
    print('Getting things ready...')
    g1 = utility.readjsondatafromfile(GRAM1FILE)
    for word, weight in g1.items():
        if len(word) > MAX_WORD_LENGTH and word != 'max_value' and word != 'min_value': continue
        data_to_write[word] = math.log10(weight)
    del g1
    print('Loading: 1/3')
    g2 = utility.readjsondatafromfile(GRAM2FILE)
    for word, d in g2.items():
        for last_one, weight in d.items():
            if len(word) > MAX_WORD_LENGTH or len(last_one) > MAX_WORD_LENGTH:
                continue
            data_to_write['{}_{}'.format(last_one, word)] = math.log10(weight)
    del g2
    print('Loading: 2/3')
    g3 = utility.readjsondatafromfile(GRAM3FILE)
    for word, d in g3.items():
        for last_one, d2 in d.items():
            for last_last_one, weight in d2.items():
                if len(word) > MAX_WORD_LENGTH or \
                        len(last_one) > MAX_WORD_LENGTH or \
                        len(last_last_one) > MAX_WORD_LENGTH:
                    continue
                data_to_write['{}_{}_{}'.format(last_last_one, last_one,
                                                word)] = math.log10(weight)
    del g3
    print('Loading: 3/3, Done!')


def _writePYDatabase():
    coding = 'gb18030'
    pyData = utility.readjsondatafromfile(PY2WORDSFILE)
    print('Start writing pinyin to words data into Sqlite')
    if os.path.exists(PY_DATABASE):
        os.remove(PY_DATABASE)
    con = sqlite3.connect(PY_DATABASE)
    cur = con.cursor()
    for i in range(1, MAX_WORD_LENGTH + 1):
        tableStr = ''
        for n in range(1, i + 1):
            tableStr += ' p{} INTEGER not null,'.format(n)
        cur.execute('create table "w{}" ({}Words BLOB not null )'.format(
            i, tableStr))
    cur.close()
    con.commit()
    cursor = con.cursor()
    pbar = tqdm.tqdm(total=len(pyData) * 2)
    pyList = {}

    for py, words in pyData.items():
        pbar.update()
        if len(py.split("'")) > MAX_WORD_LENGTH: continue
        pyList[py] = '_'.join(words)
    del pyData

    for pys, value in pyList.items():
        pbar.update()
        py_list = []
        invalid_py = False
        for py in pys.split("'"):
            py = py.replace('ve', 'ue')
            if py in res.pinyin_data.s2i_dict:
                py_list.append(res.pinyin_data.s2i_dict[py])
            else:
                py_list.append(0)
                # print('encounter null py: {0}, bypassing'.format(py))
                invalid_py = True
        if invalid_py: continue

        w = len(py_list)
        valueStr = ''
        nameStr = ''
        for i in range(1, w + 1):
            nameStr += ' p{0},'.format(str(i))
            valueStr += ' "{0}",'.format(str(py_list[i - 1]))
        cursor.execute(
            'insert into "w{0}" ({1}Words) values ({2}?)'.format(
                str(w), nameStr, valueStr), [value.encode(encoding=coding)])
    con.commit()
    for i in range(1, MAX_WORD_LENGTH + 1):
        indexStr = ''
        for n in range(1, i + 1):
            indexStr += ' "p{0}" DESC,'.format(n)
        cursor.execute('create index "w{0}_index" on "w{0}" ({1})'.format(
            i, indexStr[:-1]))
    cursor.close()
    con.commit()
    pbar.close()
    con.close()
    print('Done!')


def writeLMDB():
    p = multiprocessing.Process(target=_writePYDatabase)
    p.start()

    coding = 'gb18030'
    if len(data_to_write) == 0:
        print('💁 There is no cache exists, generating new data...')
        _get_data_ready()
    print('Start writing into LMDB')
    for file in [LMDB_FILE, LMDB_FILE + '-lock']:
        if os.path.exists(file):
            os.remove(file)

    pbar = tqdm.tqdm(total=len(data_to_write))
    env = lmdb.open(LMDB_FILE+'-tmp', map_size=104857600000, lock=False, subdir=False)

    txn = env.begin(write=True)
    for str_key, value in data_to_write.items():
        pbar.update()
        key = str_key.encode(encoding=coding)
        if isinstance(value, str):
            if len(str_key.split("'")) > 13: continue
            v = value.encode(encoding=coding)
        if isinstance(value, float):
            v = struct.pack("d", value)
        txn.put(key, v)
    txn.commit()
    env.copy(LMDB_FILE, compact=True)
    env.close()
    os.remove(LMDB_FILE+'-tmp')
    pbar.close()

    if p.is_alive():
        p.join()
    print('🎉️ All done!')