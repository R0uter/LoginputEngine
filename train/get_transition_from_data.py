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
import lmdb

PROCESS_NUM = 5
MEMORY_LIMIT_GB = 20 / PROCESS_NUM
DATA_TXT_FILE = './result_files/data.txt'
WORD_FREQ = './result_files/word_freq.txt'

PY2WORDSFILE = './result_files/pinyin2words_raw.json'
GRAM1FILE = './result_files/1gram_count.json'
GRAM2FILE = './result_files/2gram_count.json'
GRAM3FILE = './result_files/3gram_count.json'

transition_1gram_data = {}
transition_2gram_data = {}
transition_3gram_data = {}

kTransition1gram = '/1gram_transition_count.mdb'
kTransition2gram = '/2gram_transition_count.mdb'
kTransition3gram = '/3gram_transition_count.mdb'
kMMKV_DATABASE = './result_files/transition_count'

kGB18030 = 'gb18030'

last_time_flush_check = datetime.datetime.now()


def _produce_database_for(env_path):
        global last_time_flush_check
        env = lmdb.open(kMMKV_DATABASE + env_path,
                        536870912000,
                        subdir=False,
                        writemap=True,
                        map_async=True,
                        lock=False)

        length = env.stat()["entries"]
        pbar = tqdm.tqdm(total=length)

        with open("{}/{}.txt".format(kMMKV_DATABASE, env_path), 'w') as f, env.begin(write=True) as t:
            cur = t.cursor()
            for k,v in cur:
                f.write("{}\t{}\n".format(k.decode(), struct.unpack('i', v)[0]))
                pbar.update()

            cur.close()

        pbar.close()
        env.close()
        gc.collect()

        os.remove(kMMKV_DATABASE + env_path)
        os.remove(kMMKV_DATABASE + env_path + '-lock')


def _data_to_mdb(data:dict, mdb:str, pbar:tqdm):
    env = lmdb.open(kMMKV_DATABASE + mdb,
                    536870912000,
                    subdir=False,
                    writemap=True,
                    map_async=True,
                    readahead=False,
                    lock=True)

    with env.begin(write=True) as t:
        for word, count in data.items():
            pbar.update()

            key = word.encode()
            try:
                c = t.get(key)
                if c is not None:
                    count += struct.unpack('i', c)[0]
                t.put(key, struct.pack("i", count), dupdata=False)
            except lmdb.BadValsizeError:
                print('遇到超长转移{}，跳过...'.format(word))
            except BaseException as e:
                print(e)
    env.close()

def flush_if_needed(force: bool = False):
    global transition_1gram_data, transition_2gram_data, transition_3gram_data, last_time_flush_check
    # 每隔 10 分钟才检查一次内存，避免频繁检查内存占用消耗资源
    if (datetime.datetime.now() -
            last_time_flush_check).seconds <= (10 * 60) and not force:
        return
    last_time_flush_check = datetime.datetime.now()
    if utility.get_current_memory_gb() < MEMORY_LIMIT_GB and not force: 
        print('|---Current memory alloc: ', int(utility.get_current_memory_gb()))
        print('|---Needs flush to disk: ',
          utility.get_current_memory_gb() >= MEMORY_LIMIT_GB, 'Force to: ',
          force)
        return

    print('|---Current memory alloc: ', int(utility.get_current_memory_gb()))
    print('|---Needs flush to disk: ',
          utility.get_current_memory_gb() >= MEMORY_LIMIT_GB, 'Force to: ',
          force)
    pid = os.getpid()
    print('|---🚽 gram1:{}, gram2:{}, gram3:{}, pid:{}, Flushing...'.format(len(transition_1gram_data), 
                                                                            len(transition_2gram_data), 
                                                                            len(transition_3gram_data), 
                                                                            pid))


    pbar = tqdm.tqdm(total=len(transition_1gram_data)+len(transition_2gram_data)+len(transition_3gram_data))

    _data_to_mdb(transition_1gram_data, kTransition1gram, pbar)
    transition_1gram_data.clear()
    _data_to_mdb(transition_2gram_data, kTransition2gram, pbar)
    transition_2gram_data.clear()
    _data_to_mdb(transition_3gram_data, kTransition3gram, pbar)
    transition_3gram_data.clear()

    pbar.close()
    gc.collect()
    print('|---🧻 Done, now memory alloc: {:.2f}'.format(utility.get_current_memory_gb()))



def tmp_to_database():

    print('Producing database for Uni Gram')
    p1 = multiprocessing.Process(target=_produce_database_for, args=(kTransition1gram,))

    print('Producing database for Bi Gram')
    p2 = multiprocessing.Process(target=_produce_database_for, args=(kTransition2gram,))

    print('Producing database for Tri Gram')
    p3 = multiprocessing.Process(target=_produce_database_for, args=(kTransition3gram,))

    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()

# 分段处理语料文件，并发多进程加快处理速度
def processing_line(q:multiprocessing.Queue, filePath:str, start:int, end:int, process_num:int = 10, mem_limit_gb:int = 10):
    utility.load_user_data_jieba()
    utility.load_user_data_pypinyin()

    global PROCESS_NUM, MEMORY_LIMIT_GB
    PROCESS_NUM = process_num
    MEMORY_LIMIT_GB = mem_limit_gb / PROCESS_NUM

    f = open(filePath,'r',encoding='gb18030')
    f.seek(start,0)

    processingCount = 0

    while True:
        processingCount += 1
        try:
            if f.read(1) == '\n':break
        except:
            continue
    lastTime = datetime.datetime.now()

    while True:
        line = f.readline()
        processingCount += len(line)

        process_line(line)

        if (datetime.datetime.now() - lastTime).seconds >= 3:
            q.put(processingCount, False)
            processingCount = 0
            lastTime = datetime.datetime.now()

        if f.tell() >= end:break

    f.close()
    flush_if_needed(force=True)

# 统计转移频率
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

    print('|---解压缩 Uni-Gram')
    path = "{}/{}.txt".format(kMMKV_DATABASE, kTransition1gram)

    pbar = tqdm.tqdm(total=utility.read_lines_from(path))

    with open(path, 'r') as f:
        for line in f:
            pbar.update()
            try:
                k, c = line.strip().split('\t')
            except:
                continue

            count = int(c)
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


def deleteMBD():
    shutil.rmtree(kMMKV_DATABASE, True)


def process(process_num:int = 10, mem_limit_gb:int = 10):
    if not os.path.exists(kMMKV_DATABASE):
        os.makedirs(kMMKV_DATABASE)
    print('💭开始统计语料总数...')

    jobs = []
    queue = multiprocessing.Queue(10000)

    length = os.path.getsize(DATA_TXT_FILE)

    print('''
        🤓 文本总长度 {} 字符，共启动 {} 进程，每个进程分段长度 {}

        正在启动进程...
        '''.format(length, process_num, int(length/process_num)))
    # 将语料进行分片处理，平均分给所有进程。
    for i in range(0, PROCESS_NUM):
        p = multiprocessing.Process(target=processing_line, args=(queue,
                                                                  DATA_TXT_FILE,
                                                                  int(length/process_num) * i,
                                                                  int(length/process_num) * (i+1),
                                                                  process_num, mem_limit_gb ))
        jobs.append(p)
        p.start()

    pbar = tqdm.tqdm(total=length/2)

    for p in jobs:
        while p.is_alive():
            try:
                count = queue.get(False)
                pbar.update(count)
                time.sleep(1)
            except:
                pass

    pbar.close()

    print('合并统计数据')
    tmp_to_database()
    print('生成拼音到词条的数据')
    gen_py_words_json()
    print('🎉️完成！')

