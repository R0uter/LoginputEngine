import gc
import operator
import os
import struct
import time
import tqdm
import utility
import multiprocessing
import datetime
import shutil
import mmkv

PROCESS_NUM = 5
MEMORY_LIMIT_GB = 20 / PROCESS_NUM
DATA_TXT_FILE = './result_files/data.txt'
WORD_FREQ = './result_files/word_freq.txt'

PY2WORDSFILE = './result_files/pinyin2words_raw.json'
GRAM1FILE = './result_files/1gram_count.json'
GRAM2FILE = './result_files/2gram_count.json'
GRAM3FILE = './result_files/3gram_count.json'

GRAMTEMP_DIR = './result_files/temp'
GRAM1TEMP_FILE = './result_files/temp/gram1'
GRAM2TEMP_FILE = './result_files/temp/gram2'
GRAM3TEMP_FILE = './result_files/temp/gram3'

transition_1gram_data = {}
transition_2gram_data = {}
transition_3gram_data = {}

kTransition1gram = '1gram_transition_count'
kTransition2gram = '2gram_transition_count'
kTransition3gram = '3gram_transition_count'
kMMKV_DATABASE = './result_files/transition_count'

kGB18030 = 'gb18030'
kEndProcess = '-=-=-=-EOF=-=-=-=-'
last_time_flush_check = datetime.datetime.now()


def _produce_database_for(paths, env_path):
        kv = mmkv.MMKV(env_path)
        all = 0
        for p in paths:
            all += utility.read_lines_from(p)
        pbar = tqdm.tqdm(total=all)
        for p in paths:
            with open(p, mode='r', encoding=kGB18030) as f:
                for line in f:
                    pbar.update()
                    try:
                        word, valueStr = line.strip().split('\t')
                        count = int(valueStr)
                    except:
                        continue

                    c = kv.getInt(word)
                    kv.set(count+c, key=word)

        pbar.close()
        gc.collect()


# 直接读取缓存并在内存中处理合并，然后一次性写入数据库（内存足够大就用这个，比较快）
def _produce_database_in_memory(paths, env_path):
        kv = mmkv.MMKV(env_path)
        data = {}
        all = 0
        for p in paths:
            all += utility.read_lines_from(p)
        pbar = tqdm.tqdm(total=all)
        for p in paths:
            with open(p, mode='r', encoding=kGB18030) as f:
                for line in f:
                    pbar.update()
                    try:
                        word, valueStr = line.strip().split('\t')
                    except:
                        continue
                    count = int(valueStr)
                    if word in data:
                        data[word] += count
                    else:
                        data[word] = count
        pbar.close()

        pbar = tqdm.tqdm(total=len(data))
        for word, count in data.items():
            pbar.update()
            kv.set(count, key=word)

        data.clear()
        del data
        pbar.close()
        gc.collect()


def flush_if_needed(force: bool = False):
    global transition_1gram_data, transition_2gram_data, transition_3gram_data, last_time_flush_check
    # 每隔 10 分钟才检查一次内存，避免频繁检查内存占用消耗资源
    if (datetime.datetime.now() -
            last_time_flush_check).seconds <= (0 * 60) and not force:
        return
    last_time_flush_check = datetime.datetime.now()
    if utility.get_current_memory_gb() < MEMORY_LIMIT_GB and not force: return
    if not os.path.exists(GRAMTEMP_DIR):
        os.makedirs(GRAMTEMP_DIR)
    print('|---Current memory alloc: ', int(utility.get_current_memory_gb()))
    print('|---Needs flush to disk: ',
          utility.get_current_memory_gb() >= MEMORY_LIMIT_GB, 'Force to: ',
          force)
    pid = os.getpid()
    print('|---🚽 gram1:{}, gram2:{}, gram3:{}, pid:{}, Flushing...'.format(len(transition_1gram_data), 
                                                                            len(transition_2gram_data), 
                                                                            len(transition_3gram_data), 
                                                                            pid))
    tail = '-{}'.format(pid)
    with open(GRAM1TEMP_FILE+tail, mode='a', encoding=kGB18030) as f:
        for word, count in transition_1gram_data.items():
            f.write('{}\t{}\n'.format(word, count))
    transition_1gram_data.clear()
    with open(GRAM2TEMP_FILE+tail, mode='a', encoding=kGB18030) as f:
        for word, count in transition_2gram_data.items():
            f.write('{}\t{}\n'.format(word, count))
    transition_2gram_data.clear()
    with open(GRAM3TEMP_FILE+tail, mode='a', encoding=kGB18030) as f:
        for word, count in transition_3gram_data.items():
            f.write('{}\t{}\n'.format(word, count))
    transition_3gram_data.clear()
    gc.collect()
    print('|---🧻 Done, now memory alloc: {:.2f}'.format(utility.get_current_memory_gb()))


def tmp_to_database():

    g1tmp_paths = []
    g2tmp_paths = []
    g3tmp_paths = []
    for root, directories, filenames in os.walk(GRAMTEMP_DIR):
        for filename in filenames:
            p = os.path.join(root, filename)
            if 'gram1' in filename:
                g1tmp_paths.append(p)
            if 'gram2' in filename:
                g2tmp_paths.append(p)
            if 'gram3' in filename:
                g3tmp_paths.append(p)

    mmkv.MMKV.initializeMMKV(kMMKV_DATABASE, mmkv.MMKVLogLevel.NoLog)

    print('Producing database for Uni Gram')
    _produce_database_in_memory(g1tmp_paths, kTransition1gram)
    # _produce_database_for(g1tmp_paths, kTransition1gram)
    print('Producing database for Bi Gram')
    # _produce_database_in_memory(g2tmp_paths, kTransition2gram)
    _produce_database_for(g2tmp_paths, kTransition2gram)
    print('Producing database for Tri Gram')
    # _produce_database_in_memory(g3tmp_paths, kTransition3gram)
    _produce_database_for(g3tmp_paths, kTransition3gram)
    shutil.rmtree(GRAMTEMP_DIR, True)

def processing_line(q: multiprocessing.Queue):
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
        process_line(s)


def process_line(s: str):
    flush_if_needed()
    words = utility.cut_line(s.strip())

    for word in words:
        transition_1gram_data.setdefault(word, 0)
        transition_1gram_data[word] += 1

    for f, t in zip(words[:-1], words[1:]):
        key = '{}_{}'.format(f, t)
        transition_2gram_data.setdefault(key, 0)
        transition_2gram_data[key] += 1

    for f, m, t in zip(words[:-2], words[1:-1], words[2:]):
        key = '{}_{}_{}'.format(f, m, t)
        transition_3gram_data.setdefault(key, 0)
        transition_3gram_data[key] += 1


def gen_py_words_json():
    print('|---生成拼音到 Gram 数据')
    transition_1gram_data.clear()
    kv = mmkv.MMKV(kTransition1gram)
    print('|---解压缩 Uni-Gram')

    ks = kv.keys()
    pbar = tqdm.tqdm(total=len(ks))

    for k in ks:
        pbar.update()
        count = kv.getInt(k)
        transition_1gram_data[k] = count
    pbar.close()

    print('|--- 写入文件...')
    target = open(WORD_FREQ, mode='w', encoding='utf8')
    gram1data = []
    for word, weight in sorted(transition_1gram_data.items(),
                               key=operator.itemgetter(1),
                               reverse=True):
        py = utility.get_pinyin_list(word)
        pys = ''.join(py)
        gram1data.append((word, "'".join(py), weight))
        target.write('{}\t{}\t{}\n'.format(word, "'".join(py), weight))
        if len(py) == 2 and utility.is_pinyin(pys):
            # 如果词是两个字，但拼音能合在一起，那么就额外添加个条目当作单字处理一次
            gram1data.append((word, pys, weight))
            target.write('{}\t{}\t{}\n'.format(word, pys, weight))
    target.close()

    py2words_data = {}
    for word, py, w in gram1data:
        py2words_data.setdefault(py, [])
        py2words_data[py].append(word)

    for py, words in py2words_data.items():
        py2words_data[py] = list(set(py2words_data[py]))

    utility.writejson2file(py2words_data, PY2WORDSFILE)
    utility.writejson2file(transition_1gram_data, GRAM1FILE)

def mdb_to_json():
    # 数据太大了，几乎不能写入 json，如果是小数据，可以用这个把生成的 mdb 转换成json
    global transition_1gram_data, transition_2gram_data, transition_3gram_data
    transition_1gram_data.clear()
    transition_2gram_data.clear()
    transition_3gram_data.clear()

    env1 = lmdb.open(kTransition1gram, 536870912000, subdir=False)
    env2 = lmdb.open(kTransition2gram, 536870912000, subdir=False)
    env3 = lmdb.open(kTransition3gram, 536870912000, subdir=False)
    print('|---解压缩 Uni-Gram')
    with env1.begin() as t:
        pbar = tqdm.tqdm(total=env1.stat()['entries'])
        for k, count in t.cursor():
            pbar.update()
            transition_1gram_data[k.decode(encoding=kGB18030)] = struct.unpack('i', count)[0]
        pbar.close()
    gc.collect()
    print('|---解压缩 Bi-Gram')
    with env2.begin() as t:
        pbar = tqdm.tqdm(total=env2.stat()['entries'])
        for k, count in t.cursor():
            pbar.update()
            s = k.decode(encoding=kGB18030)
            l, o = s.split('_')
            transition_2gram_data.setdefault(o, {})
            transition_2gram_data[o].setdefault(l, struct.unpack('i', count)[0])
        pbar.close()
    gc.collect()
    print('|---解压缩 Tri-Gram')
    with env3.begin() as t:
        pbar = tqdm.tqdm(total=env3.stat()['entries'])
        for k, count in t.cursor():
            pbar.update()
            s = k.decode(encoding=kGB18030)
            ll, l, o = s.split('_')
            transition_3gram_data.setdefault(o, {})
            transition_3gram_data[o].setdefault(l, {})
            transition_3gram_data[o][l].setdefault(ll, struct.unpack('i', count)[0])
        pbar.close()
    gc.collect()
    env1.close()
    env2.close()
    env3.close()
    utility.writejson2file(transition_1gram_data, GRAM1FILE)
    utility.writejson2file(transition_2gram_data, GRAM2FILE)
    utility.writejson2file(transition_3gram_data, GRAM3FILE)
    # deleteMBD()


def deleteMBD():
    for file in [
            kTransition1gram, kTransition2gram, kTransition3gram,
            kTransition1gram + '-lock', kTransition2gram + '-lock',
            kTransition3gram + '-lock'
    ]:
        if os.path.exists(file):
            os.remove(file)


def process(process_num:int = 10, mem_limit_gb:int = 10):
    global PROCESS_NUM, MEMORY_LIMIT_GB
    PROCESS_NUM = process_num
    MEMORY_LIMIT_GB = mem_limit_gb / PROCESS_NUM
    utility.load_user_data_jieba()
    print('💭开始统计语料总条目数...')
    total_counts = utility.read_lines_from(DATA_TXT_FILE)
    print('''
    🤓 统计完成！
    |--- 文本行数：{}
    '''.format(total_counts))

    print('👓 开始统计转移...')
    pbar = tqdm.tqdm(total=total_counts)
    deleteMBD()
    jobs = []
    queue = multiprocessing.Queue(10000)
    for _ in range(0, PROCESS_NUM):
        p = multiprocessing.Process(target=processing_line, args=(queue, ))
        jobs.append(p)
        p.start()

    f = open(DATA_TXT_FILE, encoding='gb18030')
    # 只读取需要的部分，不再一次性加载全文
    for line in f:
        pbar.update(1)
        # 挨个往子进程里送字符串进行处理
        while queue.full():
            pass
        queue.put(line)

    f.close()
    pbar.close()
    while queue.full():
        pass
    queue.put(kEndProcess)
    start_time = datetime.datetime.now()
    print('Waiting subprocess to exit')
    for p in jobs:
        while p.is_alive():
            pass
    print('Waiting temp file to database')
    tmp_to_database()
    print('Generating py to words json file')
    gen_py_words_json()
    print('Total waiting: {:.2f}'.format((datetime.datetime.now() - start_time).seconds/60/60), 'h')
    print('🎉️完成！')

