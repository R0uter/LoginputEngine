from tqdm import tqdm
import utility
import gc
import os
import multiprocessing

WORD_FREQ = './result_files/word_freq.txt'
GRAM1FILE_COUNT = './result_files/1gram_count.json'
GRAM2FILE_COUNT = './result_files/2gram_count.json'

kTransition1gram = './result_files/transition_count/1gram_transition_count.mdb.txt'
kTransition2gram = './result_files/transition_count/2gram_transition_count.mdb.txt'
kTransition3gram = './result_files/transition_count/3gram_transition_count.mdb.txt'

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
G1CUT_RATE = 0.4
G2CUT_RATE = 60
G3CUT_RATE = 60


def smooth3gram():
    data = {}
    total_count = 0
    max_count = 0
    all = 0
    pbar = tqdm(total= os.path.getsize(kTransition3gram) / 2)
    t3file = open(kTransition3gram, 'r')
    gram2data = utility.readjsondatafromfile(GRAM2FILE_COUNT)
    print('|---Counting items...')
    for line in t3file:
        pbar.update(len(line))
        _, count = line.strip().split('\t')
        max_count += int(count)
        all += 1

    pbar.close()
    pbar = tqdm(total=all)

    cut_count = max_count / all * G2CUT_RATE

    print('|---Now removing any item that below ', max_count)
    t3file.seek(0,0)

    for line in t3file:
        pbar.update()
        k, c = line.strip().split('\t')
        count = int(c)
        if count < cut_count: continue
        f, m, t = k.split('_')
        if len(f) == 0 or len(t) == 0 or len(m) == 0: continue
        if m not in gram2data or f not in gram2data[m]: continue
        data.setdefault(t, {})
        data[t].setdefault(m, {})
        data[t][m][f] = count / gram2data[m][f]
        total_count += 1
    data['_'] = {}
    data['_']['_'] = {}
    data['_']['_']['_'] = 1/max_count
    utility.writejson2file(data, GRAM3FILE)
    pbar.close()
    t3file.close()
    data.clear()
    gc.collect()
    print('Tri-gram data count:', all, ' → ', total_count)


def smooth2gram():
    data = {}
    count_data = {}
    total_count = 0
    max_count = 0
    print('|---Counting items...')
    all = 0
    pbar = tqdm(total=os.path.getsize(kTransition2gram) / 2)
    t2file = open(kTransition2gram, 'r')
    gram1data = utility.readjsondatafromfile(GRAM1FILE_COUNT)
    for line in t2file:
        pbar.update(len(line))
        _, count = line.strip().split('\t')
        max_count += int(count)
        all += 1

    pbar.close()
    pbar = tqdm(total=all)

    cut_count = max_count / all * G2CUT_RATE
    print('|---Now removing any item that below ', cut_count)
    t2file.seek(0,0)

    for line in t2file:
        pbar.update()
        k, c = line.strip().split('\t')
        count = int(c)
        if count < cut_count: continue
        try: f, t = k.split('_')
        except: continue
        if len(f) == 0 or len(t) == 0: continue
        if f not in gram1data:continue
        data.setdefault(t, {})
        data[t][f] = count / gram1data[f]
        count_data.setdefault(t, {})
        count_data[t][f] = count
        total_count += 1
    data['_'] = {}
    data['_']['_'] = 1 / max_count
    utility.writejson2file(data, GRAM2FILE)
    utility.writejson2file(count_data, GRAM2FILE_COUNT)
    pbar.close()
    t2file.close()
    data.clear()
    count_data.clear()
    gc.collect()
    print('bi-gram data count:', all, ' → ', total_count)


def smooth1gram(gram1data, words_to_delete):
    data = {}
    all_count = 0
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
        max_value = max(n, max_value)

    data['min_value'] = 1/all_count
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
    data = {"lo":["咯"]}

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
    smooth1gram(gram1data, words_to_delete)
    print('Slim and smooth bi-gram data')
    smooth2gram()
    print('Slim and smooth tri-gram data')
    smooth3gram()

    print('😃 Done!')

if __name__ == '__main__':
    process()