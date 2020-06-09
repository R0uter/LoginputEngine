from tqdm import tqdm
import lmdb
import utility
import struct
import gc

WORD_FREQ = './result_files/word_freq.txt'
GRAM1FILE_COUNT = './result_files/1gram_count.json'
kTransition1gram = './result_files/1gram_transition_count.mdb'
kTransition2gram = './result_files/2gram_transition_count.mdb'
kTransition3gram = './result_files/3gram_transition_count.mdb'
GRAM1FILE = './result_files/1gram_transition.json'
GRAM2FILE = './result_files/2gram_transition.json'
GRAM3FILE = './result_files/3gram_transition.json'
PY2WORDS_RAW_FILE = './result_files/pinyin2words_raw.json'
PY2WORDSFILE = './result_files/pinyin2words.json'
kGB18030 = 'gb18030'
words_to_delete = {}
gram1data = {}
pyData = {}

# 调整这里的三个值来裁剪转移数据库大小到期望的大小，理论上剪的越少结果越好，但也要考虑到实际体积需求
# 这个比例是对应矩阵的中所有条目的平均出现次数的倍率，比如设定为 1，就是去掉比平均数低的所有条目，0.5就是比平均数一半还低的所有条目。
G1CUT_RATE = 0.5
G2CUT_RATE = 2
G3CUT_RATE = 22
# jieba with hmm
# 0.5 4 14 目前这个比例最优 large:54.9 small: 60.3 体积：168.2
# 0.5 2 22  large:57.52 small: 62.45 体积：205.8

# pkuseg
# 0.5 4 14 目前这个比例最优 large:50.6 small: 59.05 体积：150.8
# 0.5 2 22  large:52.6 small: 60.2 体积：173.6
# 0.5 2 18  large:52.36 small: 60.2 体积：185.7
# 0.5 1 15  large:53.4 small: 61.97 体积：296.4


def smooth3gram():
    data = {}

    total_count = 0
    env = lmdb.open(kTransition3gram, 536870912000, subdir=False, lock=False)
    max_count = 0
    all = env.stat()['entries']
    print('|---Counting items...')
    with env.begin() as t:
        for _, value in t.cursor():
            max_count += struct.unpack('i', value)[0]

    max_count /= all
    max_count *= G3CUT_RATE
    pbar = tqdm(total=all)
    print('|---Now removing any item that below ', max_count)
    with env.begin() as t:
        for k, v in t.cursor():
            pbar.update()
            count = struct.unpack('i', v)[0]
            if count < max_count: continue
            f, m, t = k.decode(kGB18030).split('_')
            if len(f) == 0 or len(t) == 0 or len(m) == 0: continue
            if f in words_to_delete or \
                t in words_to_delete or \
                    m in words_to_delete:
                continue
            data.setdefault(t, {})
            data[t].setdefault(m, {})
            data[t][m][f] = count / gram1data[f]
            total_count += 1

    utility.writejson2file(data, GRAM3FILE)
    pbar.close()
    env.close()
    data.clear()
    gc.collect()
    print('Tri-gram data count:', all, 'trimed count: ', total_count)


def smooth2gram():
    data = {}
    total_count = 0
    max_count = 0
    print('|---Counting items...')
    env = lmdb.open(kTransition2gram, 536870912000, subdir=False, lock=False)
    all = env.stat()['entries']
    with env.begin() as t:
        for _, value in t.cursor():
            max_count += struct.unpack('i', value)[0]

    max_count /= all
    max_count *= G2CUT_RATE
    print('|---Now removing any item that below ', max_count)

    pbar = tqdm(total=all)
    with env.begin() as t:
        for k, v in t.cursor():
            pbar.update()
            count = struct.unpack('i', v)[0]
            if count < max_count: continue
            f, t = k.decode(kGB18030).split('_')
            if len(f) == 0 or len(t) == 0: continue
            # if f in words_to_delete or t in words_to_delete: continue
            data.setdefault(t, {})
            data[t][f] = count / gram1data[f]
            total_count += 1
    env.close()
    utility.writejson2file(data, GRAM2FILE)
    pbar.close()
    data.clear()
    gc.collect()
    print('bi-gram data count:', all, 'trimed count: ', total_count)


def smooth1gram():
    data = {}
    all_count = 0
    min_value = 999999999.
    max_value = 0.
    pbar = tqdm(total=len(gram1data))

    for word, v in gram1data.items():
        pbar.update(0.5)
        if word in words_to_delete: continue
        all_count += v

    for word in list(gram1data.keys()):
        pbar.update(0.5)
        if word in words_to_delete: continue
        n = gram1data[word] / all_count
        data[word] = n
        min_value = min(n, min_value)
        max_value = max(n, max_value)

    data['min_value'] = min_value
    data['max_value'] = max_value

    pbar.close()
    utility.writejson2file(data, GRAM1FILE)
    print('uni-gram data count:', len(gram1data), 'trimed count:', len(data))


def process():
    global gram1data, pyData, words_to_delete
    print('🤟 Start to load counted data...')

    print('Loading...1/4')
    gram1data = utility.readjsondatafromfile(GRAM1FILE_COUNT)
    max_count = sum(gram1data.values()) / len(gram1data)
    max_count *= G1CUT_RATE
    print('Going to remove any item that below ', max_count, 'in uni-gram')
    for word, count in gram1data.items():
        if count < max_count:
            words_to_delete[word] = 0
    print('Loading...2/4')
    pyData = utility.readjsondatafromfile(PY2WORDS_RAW_FILE)
    print('Slim pinyin to words file')
    pbar = tqdm(total=len(pyData))
    data = {}

    for pinyin in list(pyData.keys()):
        pbar.update()
        new_words = list(pyData[pinyin])
        for word in pyData[pinyin]:
            if word in words_to_delete:
                new_words.remove(word)
        if len(new_words) > 1:
            new_words.sort(key=lambda x:gram1data[x], reverse=True)
        if len(new_words) > 0:
            data[pinyin] = new_words

    print('Loading...3/4')
    pbar.close()

    print('Slim and smooth uni-gram data')
    smooth1gram()
    print('Slim and smooth bi-gram data')
    smooth2gram()
    print('Slim and smooth tri-gram data')
    smooth3gram()
    print('Loading...4/4')
    print('Injecting custom words...')
    utility.writejson2file(data, PY2WORDSFILE)
    print('😃 Done!')

if __name__ == '__main__':
    process()