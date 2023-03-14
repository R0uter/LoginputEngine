import gc
import os
import re
import signal
import sys
import time

from zhon import hanzi
import string
import tqdm
import utility
import multiprocessing
import datetime

ARTICLE_DIR = './articles'
FILEDIR = './result_files'
DATA_TMP = './result_files/data_tmp'
DATA_TXT_FILE = './result_files/data_cuted.txt'
kGB18030 = 'gb18030'
kEndProcess = '-=-=-=-EOF=-=-=-=-'
last_time_flush_check = datetime.datetime.now()

PROCESS_NUM = 5
MEMORY_LIMIT_GB = 20 / PROCESS_NUM
ALLPUNC = '[{}{}　]'.format(hanzi.punctuation, string.printable)

lines_cache = []
jobs = []
queue = multiprocessing.Queue(500)  # smaller queue to faster exit when ctrl+c
current_idx = 0


def flush_if_needed(force=False):
    global lines_cache, last_time_flush_check
    # 每隔 10 分钟才检查一次内存，避免频繁检查消耗资源
    if (datetime.datetime.now() - last_time_flush_check).seconds <= (10 * 60) and not force: return
    last_time_flush_check = datetime.datetime.now()
    memory_alloc = utility.get_current_memory_gb()
    if memory_alloc < MEMORY_LIMIT_GB and not force: return
    data_path = DATA_TMP + '-' + str(os.getpid())
    print('|---Current memory alloc: ', int(memory_alloc))
    print('|---Needs flush to disk: ', memory_alloc >= MEMORY_LIMIT_GB, 'Force to: ', force)
    print('|---🚽 Flushing...')
    with open(data_path, 'a', encoding=kGB18030) as f:
        f.writelines(lines_cache)
    lines_cache.clear()
    gc.collect()
    print('|---🧻 Done, now memory alloc: ', int(memory_alloc))


def sub_processing_signal_handler(signal, frame):
    print('|---Sub process received SIGINT, finalizing...')


def processing_line(q: multiprocessing.Queue, process_num: int = 10, mem_limit_gb: int = 10):
    global PROCESS_NUM, MEMORY_LIMIT_GB
    signal.signal(signal.SIGINT, sub_processing_signal_handler)
    PROCESS_NUM = process_num
    MEMORY_LIMIT_GB = mem_limit_gb / PROCESS_NUM
    utility.init_hanlp()
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


def sub_process_line(s: str):
    flush_if_needed()
    line = utility.t2s(s)
    line = re.sub(ALLPUNC, '_', line)
    lines_cache.append(' '.join(utility.cut_line(line)) + '\n')


def remove_tmp_file():
    for root, directories, filenames in os.walk(FILEDIR):
        for filename in filenames:
            p = os.path.join(root, filename)
            if 'data_tmp-' in filename:
                os.remove(p)


def merge_tmp_files():
    # if os.path.exists(DATA_TXT_FILE):
    #     os.remove(DATA_TXT_FILE)
    f = open(DATA_TXT_FILE, mode='a', encoding='utf8')
    for root, directories, filenames in os.walk(FILEDIR):
        for filename in filenames:
            p = os.path.join(root, filename)
            if 'data_tmp-' in filename:
                with open(p, 'r', encoding=kGB18030) as t:
                    for line in t:
                        for sub_line in line.split('_'):
                            sub_line = sub_line.strip()
                            if len(sub_line) <= 1: continue
                            f.write(sub_line+'\n')
    f.close()
    remove_tmp_file()


def end_and_exit():
    pbar.close()
    queue.put(kEndProcess)
    print('Waiting subprocess to exit')

    for p in jobs:
        while p.is_alive():
            print('Queue is not empty yet, check again after 3s...')
            time.sleep(3)
            pass
    print('Merging tmp files...')
    merge_tmp_files()


def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    print('Subprocess still need to process the rest of the data in queue, please wait...')
    end_and_exit()
    print('\n\nCurrent index number: ', current_idx)
    sys.exit(0)


def gen_data_txt(process_num: int = 10, mem_limit_gb: int = 10):
    global current_idx
    start_line = 0
    signal.signal(signal.SIGINT, signal_handler)
    print('💭Start analysing corpus...')
    all_files = []
    total_bytes = 0
    for root, directories, filenames in os.walk(ARTICLE_DIR):
        for filename in filenames:
            p = os.path.join(root, filename)
            if p.endswith('.txt'):
                n = utility.read_bytes_from(p)
                all_files.append(p)
                total_bytes += n
    all_files = sorted(all_files)

    print('''
        |---Files：{}
        |---Total size：{}GB
        '''.format(len(all_files), round(total_bytes / 1024 / 1024 / 1024, 2)))
    remove_tmp_file()

    for _ in range(0, process_num):
        p = multiprocessing.Process(target=processing_line, args=(queue, process_num, mem_limit_gb))
        jobs.append(p)
        p.start()

    global pbar
    pbar = tqdm.tqdm(total=total_bytes, unit='B', unit_scale=True, unit_divisor=1024)

    for path in all_files:
        print('Processing file: ', path)
        file_is_gb18030 = False
        f = open(path, encoding='gb18030')
        try:
            f.readline()
            file_is_gb18030 = True
        except:
            f.close()

        if f.closed:
            f = open(path, encoding='utf8')
            try:
                f.readline()
            except:
                f.close()
        if f.closed:
            pbar.update(utility.read_bytes_from(path))
            print('Wrong encoding of file {}, skip...'.format(path))
            continue

        f.seek(0, 0)
        # 只读取需要的部分，不再一次性加载全文
        for line in f:
            current_idx += 1
            pbar.update(len(line.encode(kGB18030 if file_is_gb18030 else 'utf8')))
            if current_idx < start_line: continue
            # 挨个往子进程里送字符串进行处理
            while queue.full():
                time.sleep(0.1)
                pass
            queue.put(line)
        f.close()

    end_and_exit()
    print('Corpus analysis finished, data saved to: ', DATA_TXT_FILE)
