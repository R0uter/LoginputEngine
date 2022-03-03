import tqdm
import os
from train_kenlm import cut_data, arpa_to_lmdb
if not os.path.exists('./result_files'):
    os.makedirs('./result_files')

lmplz = 'train_kenlm/kenlm/build/bin/lmplz'
data = './result_files/data_cuted.txt'
arpa = './result_files/log.arpa'

def main():
    # 1 从 articles 目录中生成预处理好的语料
    # data_produce.gen_data_txt(process_num=6, mem_limit_gb=20)
    # 2 为 data.txt 文件分词处理
    # cut_data.cut_data()
    # 3 使用命令行调用 kenlm 训练 arpa 模型
    # os.system('{} -o 3 --verbose_header --text {}  --arpa {} --prune 0 30 50'.format(lmplz, data, arpa))
    # 4 生成最终可用模型，
    #   一个 LMDB 用来查词汇转移概率（以 10 为底的对数）
    arpa_to_lmdb.gen_emission_and_database()


def test():
    import utility
    from dag import dag_v2 as dag
    from datetime import datetime
    # dag.Database_Type = dag.kLMDB
    dag.load_data()

    pys = utility.get_pinyin_str("he'li'ji'qun'zhong'man'yi'de'fang'an")
    start = datetime.now()
    l = dag.get_candidates_from(pys, path_num=10)
    end = datetime.now()
    print('Running time:{}ms'.format((end - start).microseconds / 1000))
    for item in l:
        print('/'.join(item.path), item.score)
    import res.test
    test_data = res.test.smallData
    pbar = tqdm.tqdm(total=len(test_data))
    hit = 0
    for py, value in test_data.items():
        pbar.update()
        r = dag.get_candidates_from(py, path_num=10)
        rstr = 'None'
        if len(r) > 0:
            rstr = ''.join(r[0].path)
        if rstr == value:
            hit += 1
        if pbar.n % 100 == 0 and rstr != value and len(r) > 0:
            print("test:{}, result:{}, should:{}".format(py, '/'.join(r[0].path), value))
    print('命中率：{}%'.format(hit / len(test_data) * 100), )


if __name__ == '__main__':
    # main()
    test()

