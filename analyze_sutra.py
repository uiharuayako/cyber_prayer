import os
import re
import csv
import jieba
from collections import Counter
from tqdm import tqdm


def analyze_buddhist_text(file_path, output_csv_path, encoding='gbk'):
    # --- 1. 初始化统计变量 ---
    stats = {
        'total_chars': 0,  # 总字符数
        'chinese_chars': 0,  # 中文字数
        'punctuation': 0,  # 标点数
        'whitespace': 0,  # 空格数 (含换行符、制表符等)
        'lines': 0  # 行数
    }

    # 用于统计词频
    word_counter = Counter()

    # --- 2. 预编译正则，提高匹配效率 ---
    # 匹配中文字符范围 (基本汉字范围，涵盖繁简)
    re_chinese = re.compile(r'[\u4e00-\u9fa5]')
    # 匹配空白字符
    re_whitespace = re.compile(r'\s')
    # 匹配标点符号 (排除汉字、字母、数字、空白后的剩余字符通常视作标点)
    # 这种方式比列举所有标点更通用
    re_not_punct = re.compile(r'[\u4e00-\u9fa5\w\s]')

    # 停用词/符号过滤集合（分词后不统计这些标点为“词”）
    # 包含常见的中英文标点
    ignore_symbols = {
        '，', '。', '、', '；', '：', '？', '！', '“', '”', '‘', '’',
        '（', '）', '【', '】', '《', '》', '…', '—', '·',
        ',', '.', ';', ':', '?', '!', '"', "'", '(', ')', '[', ']', '<', '>', '-',
        '\n', '\r', '\t', ' '
    }

    print(f"正在处理文件: {file_path}")
    print("这可能需要几分钟，具体取决于CPU性能...")

    # 获取文件大小用于进度条
    file_size = os.path.getsize(file_path)

    try:
        # 使用 errors='replace' 防止因个别乱码导致程序崩溃
        with open(file_path, 'r', encoding=encoding, errors='replace') as f, \
                tqdm(total=file_size, unit='B', unit_scale=True, desc="分析进度") as pbar:

            for line in f:
                # 更新读取字节数到进度条
                # 注意：这里估算的字节数在不同行尾符下可能有微小偏差，但不影响总体进度显示
                pbar.update(len(line.encode(encoding, errors='replace')))

                # --- 基础统计 ---
                stats['lines'] += 1
                stats['total_chars'] += len(line)

                # 使用正则统计各类字符数量
                stats['chinese_chars'] += len(re_chinese.findall(line))
                stats['whitespace'] += len(re_whitespace.findall(line))

                # 标点数 = 总长 - (中文字 + 英文/数字 + 空白)
                # 或者直接反向匹配：匹配非字、非空白
                # 这里使用简单的反向过滤法统计标点
                non_punct_count = len(re_not_punct.findall(line))
                stats['punctuation'] += (len(line) - non_punct_count)

                # --- 分词与词频统计 ---
                # jieba 处理繁体中文效果通常也不错
                words = jieba.cut(line)

                # 过滤掉单字标点、空白符，只统计有意义的词
                # 如果你也想统计单个汉字（如“佛”），保留 len(w) >= 1 即可
                # 如果只想统计双字及以上词语，改用 len(w) > 1
                filtered_words = [w for w in words if w not in ignore_symbols and w.strip() != '']

                word_counter.update(filtered_words)

    except FileNotFoundError:
        print(f"错误: 找不到文件 {file_path}")
        return
    except UnicodeDecodeError as e:
        print(f"编码错误: 请确认文件是否为 {encoding} 格式。详细信息: {e}")
        return

    # --- 3. 输出直接统计结果 ---
    print("\n" + "=" * 30)
    print("【统计结果】")
    print(f"总行数: {stats['lines']:,}")
    print(f"总字符数: {stats['total_chars']:,}")
    print(f"中文字数: {stats['chinese_chars']:,}")
    print(f"标点符号数: {stats['punctuation']:,}")
    print(f"空格/换行数: {stats['whitespace']:,}")
    print("=" * 30)

    # --- 4. 导出词频到 CSV ---
    print(f"\n正在导出前 1000 个高频词到: {output_csv_path}")

    top_1000 = word_counter.most_common(1000)

    with open(output_csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['排名', '词语', '出现次数'])
        for rank, (word, count) in enumerate(top_1000, 1):
            writer.writerow([rank, word, count])

    print("完成！")


if __name__ == "__main__":
    # --- 配置区域 ---
    # 请修改这里的文件路径
    INPUT_FILE = "./book/gbk/sum_gbk.txt"  # 输入文件名
    OUTPUT_CSV = "./book/gbk/word_freq.csv"  # 输出CSV文件名

    if os.path.exists(INPUT_FILE):
        analyze_buddhist_text(INPUT_FILE, OUTPUT_CSV)
    else:
        # 为了演示，如果找不到文件，创建一个假的测试文件
        print(f"未找到 {INPUT_FILE}，正在生成测试文件以供演示...")
        with open(INPUT_FILE, 'w', encoding='gbk') as f:
            f.write("观自在菩萨，行深般若波罗蜜多时。\n照见五蕴皆空，度一切苦厄。\n")
            f.write("色不异空，空不异色，色即是空，空即是色。\n") * 10000
        print("测试文件生成完毕，开始分析...")
        analyze_buddhist_text(INPUT_FILE, OUTPUT_CSV)