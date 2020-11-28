import tqdm
import os
from train import data_produce, get_transition_from_data, get_smooth_transition
import database_generator

if not os.path.exists('./result_files'):
    os.makedirs('./result_files')
        

def main():
    # 1 从 articles 目录中生成预处理好的语料
    # data_produce.gen_data_txt(process_num=5, mem_limit_gb=15)
    # 2 从生成的 data.txt 文件统计转移
    get_transition_from_data.process(process_num=5, mem_limit_gb=15)
    # 3 对统计得出的转移词频进行修剪以缩小体积并用最大似然法平滑
    get_smooth_transition.process()
    # # 4 用平滑后的结果生成用于计算的二进制数据库，一个 SQLite 用来查拼音到词汇，
    # #   一个 LMDB 用来查词汇转移概率（以 10 为底的对数）

    database_generator.writeLMDB()

    # # 5 可选追加额外自定义词条写入词库，写入的词库按最低概率存储，仅作单词命中补充
    import train.inject
    # 这里重新生成一下拼音发射矩阵避免重复写入形成脏数据
    get_smooth_transition.gen_words2delete()
    train.inject.start()


    import utility
    from dag.dag import get_candidates_from, evalue
    from datetime import datetime


    # l = get_candidates_from("guai'ji")
    # l = get_candidates_from("he'li'ji'qun'zhong'man'yi'de'fang'an")

    pys = utility.get_pinyin_str("he'li'ji'qun'zhong'man'yi'de'fang'an")
    start = datetime.now()
    l = get_candidates_from(pys, path_num=10)
    end = datetime.now()
    print('Running time:{}ms'.format((end - start).microseconds / 1000))
    for item in l:
        print('/'.join(item.path), item.score)

    test()

def test():
    from dag.dag import get_candidates_from, evalue
    import res.test
    test_data = res.test.largefile
    pbar = tqdm.tqdm(total=len(test_data))
    hit = 0
    for py, value in test_data.items():
        pbar.update()
        r = get_candidates_from(py, path_num=10, log=True)
        rstr = 'None'
        if len(r) > 0:
            rstr = ''.join(r[0].path)
        if rstr == value:
            hit += 1
        if pbar.n % 100 == 0 and rstr != value and len(r) > 0:
            print("test:{}, result:{}, should:{}".format(py, '/'.join(r[0].path), value))
    print('命中率：{}%'.format(hit/len(test_data)*100),)


if __name__ == '__main__':
    main()
