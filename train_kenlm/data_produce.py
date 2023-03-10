import gc
import os
import re
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
DATA_TXT_FILE = './result_files/data.txt'
kGB18030 = 'gb18030'
kEndProcess = '-=-=-=-EOF=-=-=-=-'
last_time_flush_check = datetime.datetime.now()

PROCESS_NUM = 5
MEMORY_LIMIT_GB = 20 / PROCESS_NUM
ALLPUNC = '[{}{}{}]'.format(hanzi.punctuation,string.whitespace,"A-Za-z!\"#\$\%\&'\(\)\*\+\,-\.\/:;<=>\?@\[\]\^_`\{\|\}~◆●")  # 保留数字

lines_cache = []


def flush_if_needed(force=False):
    global lines_cache, last_time_flush_check
    # 每隔 10 分钟才检查一次内存，避免频繁检查消耗资源
    if (datetime.datetime.now() - last_time_flush_check).seconds <= (10 * 60) and not force: return
    last_time_flush_check = datetime.datetime.now()
    memory_alloc = utility.get_current_memory_gb()
    if memory_alloc < MEMORY_LIMIT_GB and not force:return
    data_path = DATA_TMP+'-'+str(os.getpid())
    print('|---Current memory alloc: ', int(memory_alloc))
    print('|---Needs flush to disk: ', memory_alloc >= MEMORY_LIMIT_GB, 'Force to: ', force)
    print('|---🚽 Flushing...')
    with open(data_path, 'a', encoding=kGB18030) as f:
        f.writelines(lines_cache)
    lines_cache.clear()
    gc.collect()
    print('|---🧻 Done, now memory alloc: ', int(memory_alloc))


def processing_line(q: multiprocessing.Queue, process_num:int = 10, mem_limit_gb:int = 10):
    global PROCESS_NUM, MEMORY_LIMIT_GB
    PROCESS_NUM = process_num
    MEMORY_LIMIT_GB = mem_limit_gb / PROCESS_NUM
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
    line = utility.t2s(s)
    line = re.sub(ALLPUNC, '_', line)
    lines = line.split('_')
    for subline in lines:
        if len(subline) <= 1: continue  # if the line is too short, skip it. We need at least 2 characters
        try:
            float(subline)
            continue  # if the line is number only, skip it
        except ValueError:
            pass
        lines_cache.append(subline + '\n')


def remove_tmp_file():
    for root, directories, filenames in os.walk(FILEDIR):
        for filename in filenames:
            p = os.path.join(root, filename)
            if 'data_tmp-' in filename:
                os.remove(p)


def sumup_tmp_files():
    if os.path.exists(DATA_TXT_FILE):
        os.remove(DATA_TXT_FILE)
    f = open(DATA_TXT_FILE, mode='a', encoding='utf8')
    for root, directories, filenames in os.walk(FILEDIR):
        for filename in filenames:
            p = os.path.join(root, filename)
            if 'data_tmp-' in filename:
                with open(p, 'r', encoding=kGB18030) as t:
                    for line in t:
                        f.write(line)
    f.close()
    remove_tmp_file()


def gen_data_txt(process_num:int = 10, mem_limit_gb:int = 10):


    print('💭开始统计资料总条目数...')
    all_files = []
    total_counts = 0
    for root, directories, filenames in os.walk(ARTICLE_DIR):
        for filename in filenames:
            p = os.path.join(root, filename)
            if p.endswith('.txt'):
                n = utility.read_lines_from(p)
                if n == -1:
                    print(p, '⚠️ Wrong encoding!')
                    continue
                all_files.append(p)
                total_counts += n
    print('''
        🤓 统计完成！
        |---文件数：{}
        |---文本行数：{}
        '''.format(len(all_files), total_counts))
    remove_tmp_file()
    pbar = tqdm.tqdm(total=total_counts)
    queue = multiprocessing.Queue(10000)
    jobs = []
    for _ in range(0, PROCESS_NUM):
        p = multiprocessing.Process(target=processing_line, args=(queue, process_num, mem_limit_gb))
        jobs.append(p)
        p.start()

    for path in all_files:
        f = open(path, encoding='gb18030')
        try:
            line = f.readline()
        except:
            f.close()

        if f.closed:
            f = open(path, encoding='utf8')
            try:
                line = f.readline()
            except:
                f.close()
        if f.closed:
            print('Wrong encoding of file {}, bypassing...'.format(path))
            continue
        del line
        f.seek(0, 0)
        # 只读取需要的部分，不再一次性加载全文
        for line in f:
            pbar.update(1)
            # 挨个往子进程里送字符串进行处理
            while queue.full():
                pass
            queue.put(line)
        f.close()

    pbar.close()

    queue.put(kEndProcess)
    print('Waiting subprocess to exit')
    for p in jobs:
        while p.is_alive():
            pass
    print('合并缓存……')
    sumup_tmp_files()
    print('🗃 语料预处理完成！')
