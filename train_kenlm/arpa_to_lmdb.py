import operator
import os
# import mmkv mmkv不可取，iOS中不能使用只读模式
import lmdb
import utility
import tqdm
import struct
import sqlite3
import res.pinyin_data

ARPA_PATH = './result_files/log.arpa'
word_file = './res/word.txt'
LMDB_FILE = './result_files/transition_v2.mdb'
PY_DATABASE = './result_files/emission_v2.db'
data_to_write = {}
vocab = {} # add a vocab to remove unused words in arpa

MAX_WORD_LENGTH = 8

def quantize_floats(floats, bits=8, min_val=-10.0, max_val=0.0):
    """
    将浮点数列表量化为整数，支持回退权重为 None，明确小端字节序。
    :param floats: 输入浮点数列表，例如 [-1.1111, -2.2222] 或 [-1.1111, None]
    :param bits: 量化位数（8 或 16）
    :param min_val: 浮点数最小值
    :param max_val: 浮点数最大值
    :return: 量化后的整数列表和打包的二进制字节流（小端）
    """
    if bits not in [8, 16]:
        raise ValueError("只支持 8 位或 16 位量化")

    # 计算量化范围
    max_int = (1 << bits) - 1  # 8 位: 255, 16 位: 65535
    scale = max_int / (max_val - min_val)
    offset = min_val

    # 量化
    quantized = []
    for f in floats:
        if f is None:
            continue
        q = int((f - offset) * scale)
        q = max(0, min(max_int, q))  # 限制范围
        quantized.append(q)

    # 打包成二进制，明确小端字节序
    if bits == 8:
        if len(quantized) == 1:
            binary = struct.pack('<B', quantized[0])  # 小端，单个 8 位整数
        else:
            binary = struct.pack('<BB', *quantized)  # 小端，2 个 8 位整数
    else:  # 16 位
        if len(quantized) == 1:
            binary = struct.pack('<H', quantized[0])  # 小端，单个 16 位整数
        else:
            binary = struct.pack('<HH', *quantized)  # 小端，2 个 16 位整数

    return quantized, binary

def _get_data_ready():
    print('Loading Arpa...')
    gram1count = 0
    gram2count = 0
    gram3count = 0
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
                if float(weight) < -6.5: continue
                if word not in vocab: continue
                data_to_write[word] = (float(weight), float(bow))
                gram1count += 1

            if reading_gram == 2:
                if 's>' in line:  # 带有 s> 的是开头或者结尾，我们不需要
                    continue
                weight, words, bow = line.strip().split('\t')
                if float(weight) < -2.1: continue
                if False in [word in vocab for word in words.split(' ')]:
                    continue
                words = words.replace(' ', '_')
                data_to_write[words] = (float(weight), float(bow))
                gram2count += 1

            if reading_gram == 3:
                if 's>' in line:  # 带有 s> 的是开头或者结尾，我们不需要
                    continue
                try:
                    weight, words = line.strip().split('\t')
                    if float(weight) < -0.0: continue
                except:
                    print(line)
                    continue
                if False in [word in vocab for word in words.split(' ')]:
                    continue
                words = words.replace(' ', '_')
                data_to_write[words] = (float(weight), None)
                gram3count += 1

    print('Loading Arpa Done!')
    print('1gram count: ', gram1count)
    print('2gram count: ', gram2count)
    print('3gram count: ', gram3count)


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
                if len(word) > 4: continue
                vocab[word] = 0
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
            try:
                word, py = line.strip().split('\t')
            except ValueError:
                print('wrong line:', line)
            if len(word) > 8: continue
            if len(word) <= 4: vocab[word] = 0
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
    _write_py_database() # this also build the vocab for later use
    _get_data_ready() # read arpa and filter out based on weight and vocab
    print('Start writing into LMDB')
    coding = 'gb18030'
    for file in [LMDB_FILE, LMDB_FILE + '-lock']:
        if os.path.exists(file):
            os.remove(file)

    pbar = tqdm.tqdm(total=len(data_to_write))
    env = lmdb.open(LMDB_FILE + '-tmp', map_size=104857600000, lock=False, subdir=False)

    txn = env.begin(write=True)
    for str_key, value in data_to_write.items():
        pbar.update()
        key = str_key.encode(encoding=coding)
        quantized, v = quantize_floats(value, 8)
        txn.put(key, v)
    txn.commit()
    env.copy(LMDB_FILE, compact=True)
    env.close()
    os.remove(LMDB_FILE + '-tmp')
    pbar.close()

    print('🎉️ All done!')

