import sqlite3
import sys
from dag import dag
import utility
from tqdm import tqdm


kGB18030 = 'gb18030'


word_file = './res/word.txt'
PY2WORDSFILE = './result_files/pinyin2words.json'
pinyin_words = {}

def _read_words():
    with open(word_file, 'r') as f:
        for line in f:
            word, py = line.strip().split('\t')
            if len(word) > 8:continue
            pinyin_words.setdefault(py, [])
            pinyin_words[py].append(word)


def start(pyData):
    _read_words()
    dag.Database_Type = dag.kRAWDATA
    dag.load_data()
    for py, words in pinyin_words.items():
        if len(words) > 1:
            pinyin_words[py].sort(key=lambda x:dag.evalue(x,path_num=3, log=False), reverse=True)
    pbar = tqdm(total=len(pinyin_words))
    for py, words in pinyin_words.items():
        pbar.update()
        if py in pyData:
            old_words = pyData[py] 
            pyData[py] = list(dict.fromkeys(old_words+words))
        else:
            pyData[py] = words
    pbar.close()


def test():
    pyData = utility.readjsondatafromfile(PY2WORDSFILE)
    start(pyData)
    utility.writejson2file(pyData, PY2WORDSFILE)




