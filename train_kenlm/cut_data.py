import tqdm
import utility
import gc

DATA_PATH = './result_files/data.txt'
FILEDIR = './result_files'
DATA_TXT_FILE = './result_files/data_cuted.txt'
kGB18030 = 'gb18030'


def cut_data():
    utility.init_hanlp()
    print('💭开始统计资料总条目数...')
    total_counts = utility.read_lines_from(DATA_PATH)

    print('''
        🤓 统计完成！
        |---文本行数：{}
        '''.format(total_counts))

    pbar = tqdm.tqdm(total=total_counts)
    target = open(DATA_TXT_FILE, 'w', encoding='utf8')
    content = []
    with open(DATA_PATH, 'r', encoding=kGB18030) as f:
        for line in f:
            pbar.update(1)
            content.append(' '.join((utility.cut_line(line))))
            if len(content) < 1000_0000: continue

            for cut_line in content:
                target.write(cut_line)
            content = []
            gc.collect()
    for cut_line in content:
        target.write(cut_line)
    pbar.close()
    target.close()
    print('🗃 语料分词处理完成！')
