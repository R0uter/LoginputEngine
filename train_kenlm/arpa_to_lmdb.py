import operator
import os
# import mmkv mmkv不可取，iOS中不能使用只读模式
import lmdb
import utility
import tqdm
import struct
import sqlite3
import res.pinyin_data
import multiprocessing

ARPA_PATH = './result_files/log.arpa'
word_file = './res/word.txt'
LMDB_FILE = './result_files/transition_v2.mdb'
PY_DATABASE = './result_files/emission_v2.db'
data_to_write = {}

MAX_WORD_LENGTH = 8


def _get_data_ready():
    print('Loading Arpa...')
    with open(ARPA_PATH, 'r') as f:
        reading_gram = 0

        for line in f:
            if '\xa0' in line: continue

            if reading_gram == 0 and line.startswith('\\1-grams:'):
                reading_gram = 1
                data_to_write['<unk>'] = [-8.499597, 0.0]
                continue

            if reading_gram == 0 and line.startswith('\\2-grams:'):
                reading_gram = 2
                continue

            if reading_gram == 0 and line.startswith('\\3-grams:'):
                reading_gram = 3
                continue

            if line == '\n':
                reading_gram = 0
                continue

            if 'end' in line:
                break  # end of file

            if reading_gram == 1:
                weight, word, bow = line.strip().split('\t')
                if float(weight) < -6: continue
                data_to_write[word] = (float(weight), float(bow))

            if reading_gram == 2:
                if 's>' in line:  # 带有 s> 的是开头或者结尾，我们不需要
                    continue
                weight, words, bow = line.strip().split('\t')
                if float(weight) < -1: continue
                words = words.replace(' ', '_')
                data_to_write[words] = (float(weight), float(bow))

            if reading_gram == 3:
                if 's>' in line:  # 带有 s> 的是开头或者结尾，我们不需要
                    continue
                try:
                    weight, words = line.strip().split('\t')
                    if float(weight) < -0.5: continue
                except:
                    print(line)
                    continue
                words = words.replace(' ', '_')
                data_to_write[words] = (float(weight), None)

    print('Loading Arpa Done!')


def _write_py_database():
    utility.load_user_data_pypinyin()
    gram1_raw_data = {}
    print('Start loading 1gram for py data')
    with open(ARPA_PATH, 'r') as f:
        start_reading = False

        for line in f:
            if not start_reading and line.startswith('\\1-grams:'):
                start_reading = True
                continue

            if line == '\n' and start_reading:
                break

            if start_reading:
                if '\xa0' in line: continue
                weight, word, _ = line.split('\t')
                if float(weight) < -6: continue
                gram1_raw_data[word] = float(weight)

    pyData = {}
    for word, weight in sorted(gram1_raw_data.items(), key=operator.itemgetter(1), reverse=True):
        py = utility.get_pinyin_list(word)
        pys = ''.join(py)
        pyData.setdefault("'".join(py), [])
        pyData["'".join(py)].append(word)
        if len(py) == 2 and utility.is_pinyin(pys):
            # 如果词是两个字，但拼音能合在一起，那么就额外添加个条目当作单字处理一次
            pyData.setdefault(pys, [])
            pyData[pys].append(word)

    with open(word_file, 'r') as f:
        for line in f:
            word, py = line.strip().split('\t')
            if len(word) > 8: continue
            pyData.setdefault(py, [])
            if word not in pyData[py]:
                pyData[py].append(word)

    coding = 'gb18030'
    print('Writing pinyin to words data into Sqlite')
    if os.path.exists(PY_DATABASE):
        os.remove(PY_DATABASE)
    con = sqlite3.connect(PY_DATABASE)
    cur = con.cursor()
    for i in range(1, MAX_WORD_LENGTH + 1):
        tableStr = ''
        for n in range(1, i + 1):
            tableStr += ' p{} INTEGER not null,'.format(n)
        cur.execute('create table "w{}" ({}Words BLOB not null )'.format(i, tableStr))
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


def gen_emission_and_database():
    p = multiprocessing.Process(target=_write_py_database)
    p.start()

    _get_data_ready()
    coding = 'gb18030'

    print('Start writing into LMDB')

    for file in [LMDB_FILE, LMDB_FILE + '-lock']:
        if os.path.exists(file):
            os.remove(file)

    pbar = tqdm.tqdm(total=len(data_to_write))
    env = lmdb.open(LMDB_FILE + '-tmp', map_size=104857600000, lock=False, subdir=False)

    txn = env.begin(write=True)
    for str_key, value in data_to_write.items():
        pbar.update()
        key = str_key.encode(encoding=coding)

        v = struct.pack("<d", value[0])
        if value[1] is not None:
            v2 = struct.pack("<d", value[1])
            txn.put(key, v + v2)
        else:
            txn.put(key, v)
    txn.commit()
    env.copy(LMDB_FILE, compact=True)
    env.close()
    os.remove(LMDB_FILE + '-tmp')
    pbar.close()

    if p.is_alive():
        p.join()

    p.close()

    print('🎉️ All done!')
