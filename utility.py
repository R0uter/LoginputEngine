import gc
import os
import psutil
from pypinyin import Style, pinyin
import json
import plistlib
import jieba as jieba
from opencc import OpenCC
from res import pinyin_data
import re
from zhon import hanzi

cc = OpenCC('t2s')

jieba.load_userdict('./res/new_words.txt')
# jieba.enable_paddle()

special_py_list = ['ao', 'ai', 'ie', 'ue', 'an']
# 声母
__shengmu = {'b','p','m','f','d','t','n','l','g','k','h','j','q','x','zh','ch','sh','r','z','c','s','w','y'}


def is_shengmu(v):
    return v in __shengmu


def get_shengmu(one_py):
    if len(one_py) == 0:
        return None
    elif len(one_py) == 1:
        if is_shengmu(one_py):
            return one_py
        else:
            return None
    else:
        if is_shengmu(one_py[:2]):
            return one_py[:2]
        elif is_shengmu(one_py[:1]):
            return one_py[:1]
        else:
            return None


def is_chinese(s: str) -> bool:
    if len(re.findall(hanzi.sentence, s)) != 0:
        return True
    return False


def is_pinyin(py: str) -> bool:
    return py in pinyin_data.s2i_dict


def t2s(s: str) -> str:
    return cc.convert(s)


def cut_line(s: str) -> [str]:
    # return jieba.lcut(s, use_paddle=True)
    return list(jieba.cut(s, cut_all=False, HMM=True))


def get_pinyin_list(word):
    r = pinyin(word, style=Style.NORMAL, strict=False)
    a = []
    for w in r:a.append(w[0])
    return a


def get_pinyin_str(word):
    return "'".join(get_pinyin_list(word))


def writePlist2File(obj, filename):
    with open(filename, 'wb') as out:
        plistlib.dump(obj, out, fmt=plistlib.PlistFormat.FMT_BINARY)


def writejson2file(data, filename):
    # with open(filename, 'w') as outfile:
    #     data = json.dumps(data, indent=4, sort_keys=True)
    #     outfile.write(data)
    with open(filename, 'w', encoding='utf8') as f:
        for chunk in json.JSONEncoder(indent=4, sort_keys=True, ensure_ascii=False).iterencode(data):
            f.write(chunk)


def readjsondatafromfile(filename):
    with open(filename) as outfile:
        return json.load(outfile)


def read_lines_from(path: str) -> int:
    num = 0
    with open(path, encoding='gb18030') as f:
        try:
            for _ in f:
                num += 1
                if num % 1000000 == 0:
                    print('\r{}'.format(num), end='', flush=True)
        except:
            pass
    with open(path, encoding='utf8') as f:
        try:
            for _ in f:
                num += 1
                if num % 1000000 == 0:
                    print('\r{}'.format(num), end='', flush=True)
        except:
            pass
    gc.collect()
    return num


def get_current_memory_gb() -> int:
    pid = os.getpid()
    p = psutil.Process(pid)
    # 获取当前进程内存占用，如果快满了就写到硬盘里。
    info = p.memory_full_info()
    return info.uss / 1024. / 1024. / 1024.