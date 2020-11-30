from tqdm import tqdm
import utility
import gc
import os
import multiprocessing

WORD_FREQ = './result_files/word_freq.txt'
GRAM1FILE_COUNT = './result_files/1gram_count.json'

kTransition1gram = './result_files/transition_count/1gram_transition_count.txt'
kTransition2gram = './result_files/transition_count/2gram_transition_count.txt'
kTransition3gram = './result_files/transition_count/3gram_transition_count.txt'

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
G2CUT_RATE = 40
G3CUT_RATE = 55

# jieba+words with hmm 加上新闻语料超大词库
# 1 25 16 large:86.7 small: 91.9 体积：144.3
# 0.7 30 35 large:86.9 small: 91.48 体积：77.6
# 0.7 40 45 large:86.8 small: 91.48 体积：56.5
# 1 50 55 large:86.65 small: 91.47 体积：41.7
# 1 70 75 目前这个比例最优 large:86.44 small: 91.48 体积：30.4





def smooth3gram(gram1data):
    data = {}

    total_count = 0
    max_count = 0

    all = os.path.getsize(kTransition3gram)
    pbar = tqdm(total= all * 2)
    f = open(kTransition3gram, 'r')
    print('|---Counting items...')
    for line in f:
        pbar.update(len(line))
        _, count = line.strip().split('\t')
        max_count += int(count)

    max_count /= all
    max_count *= G3CUT_RATE

    print('|---Now removing any item that below ', max_count)
    f.seek(0,0)
    for line in f:
        pbar.update(len(line))
        k, c = line.strip().split('\t')
        count = int(c)
        if count < max_count: continue
        f, m, t = k.split('_')
        if len(f) == 0 or len(t) == 0 or len(m) == 0: continue
        data.setdefault(t, {})
        data[t].setdefault(m, {})
        data[t][m][f] = count / gram1data[f]
        total_count += 1

    utility.writejson2file(data, GRAM3FILE)
    pbar.close()
    f.close()
    data.clear()
    gc.collect()
    print('Tri-gram data count:', all, ' → ', total_count)


def smooth2gram(gram1data):
    data = {}
    total_count = 0
    max_count = 0
    print('|---Counting items...')
    all = os.path.getsize(kTransition3gram)
    pbar = tqdm(total=all * 2)
    f = open(kTransition3gram, 'r')

    for line in f:
        pbar.update(len(line))
        _, count = line.strip().split('\t')
        max_count += int(count)

    max_count /= all
    max_count *= G2CUT_RATE
    print('|---Now removing any item that below ', max_count)
    f.seek(0,0)
    for line in f:
        pbar.update(len(line))
        k, c = line.strip().split('\t')
        count = int(c)
        if count < max_count: continue
        f, t = k.split('_')
        if len(f) == 0 or len(t) == 0: continue
        data.setdefault(t, {})
        data[t][f] = count / gram1data[f]
        total_count += 1

    utility.writejson2file(data, GRAM2FILE)
    pbar.close()
    f.close()
    data.clear()
    gc.collect()
    print('bi-gram data count:', all, ' → ', total_count)


def smooth1gram(gram1data):
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
        if len(word) > 4: continue
        n = gram1data[word] / all_count
        data[word] = n
        min_value = min(n, min_value)
        max_value = max(n, max_value)

    data['min_value'] = min_value
    data['max_value'] = max_value

    pbar.close()
    utility.writejson2file(data, GRAM1FILE)
    print('uni-gram data count:', len(gram1data), ' → ', len(data))


def gen_words2delete():
    global gram1data, pyData, words_to_delete
    
    gram1data = utility.readjsondatafromfile(GRAM1FILE_COUNT)
    max_count = sum(gram1data.values()) / len(gram1data)
    max_count *= G1CUT_RATE
    print('Going to remove any item that below ', max_count, 'in uni-gram')
    for word, count in gram1data.items():
        if count < max_count:
            words_to_delete[word] = 0
    
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
            
    utility.writejson2file(data, PY2WORDSFILE)
    
    pbar.close()


def process():
    print('🤟 Start to load counted data...')
    print('Loading...1/2')
    gen_words2delete()
    print('Loading...2/2')
    print('Slim and smooth uni-gram data')
    p1 = multiprocessing.Process(target=smooth1gram, args=(gram1data,))
    print('Slim and smooth bi-gram data')
    p2 = multiprocessing.Process(target=smooth2gram, args=(gram1data,))
    print('Slim and smooth tri-gram data')
    p3 = multiprocessing.Process(target=smooth3gram, args=(gram1data,))

    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()

    print('😃 Done!')


if __name__ == '__main__':
    process()