import os
import csv
import glob
from tqdm import tqdm


def process_buddhist_scriptures(source_folder, output_txt_path, output_csv_path, encoding_type):
    """
    遍历、清洗并合并佛经文档。
    如果输出文件的目标文件夹不存在，会自动创建。
    """

    # --- 新增：自动创建输出目录 ---
    # 检查 TXT 输出路径的文件夹是否存在
    txt_dir = os.path.dirname(output_txt_path)
    if txt_dir and not os.path.exists(txt_dir):
        try:
            os.makedirs(txt_dir)
            print(f"已创建目录: {txt_dir}")
        except OSError as e:
            print(f"创建目录 {txt_dir} 失败: {e}")
            return

    # 检查 CSV 输出路径的文件夹是否存在（如果与 txt 不同）
    csv_dir = os.path.dirname(output_csv_path)
    if csv_dir and csv_dir != txt_dir and not os.path.exists(csv_dir):
        try:
            os.makedirs(csv_dir)
            print(f"已创建目录: {csv_dir}")
        except OSError as e:
            print(f"创建目录 {csv_dir} 失败: {e}")
            return
    # ---------------------------

    # 1. 扫描并收集所有 txt 文件
    print(f"正在扫描 '{source_folder}' 下的文件...")
    file_list = []

    # 使用 os.walk 递归遍历文件夹
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if file.lower().endswith('.txt'):
                full_path = os.path.join(root, file)
                # 计算相对路径
                rel_path = os.path.relpath(full_path, source_folder)
                file_list.append((full_path, rel_path))

    if not file_list:
        print("未找到任何 txt 文件。")
        return

    # 2. 按照文件相对路径的字母顺序排序
    file_list.sort(key=lambda x: x[1])

    print(f"共找到 {len(file_list)} 个文件，准备处理...")

    # 3. 打开输出文件准备写入
    try:
        with open(output_txt_path, 'w', encoding=encoding_type, errors='ignore') as f_out, \
                open(output_csv_path, 'w', encoding='utf-8-sig', newline='') as f_csv:

            # 初始化 CSV 写入器
            csv_writer = csv.writer(f_csv)
            csv_writer.writerow(['文件相对路径', '經文資訊'])  # 写入表头

            # 使用 tqdm 显示进度条
            for full_path, rel_path in tqdm(file_list, desc="处理进度", unit="file"):

                try:
                    # 读取单个源文件 (UTF-8)
                    with open(full_path, 'r', encoding='utf-8') as f_in:
                        lines = f_in.readlines()

                    meta_info = "未找到資訊"
                    header_delimiter_count = 0  # 计数遇到的 #---

                    # 处理当前文件的每一行
                    for line in lines:
                        stripped_line = line.strip()

                        # --- 任务 A: 提取【經文資訊】 ---
                        if '【經文資訊】' in line:
                            temp_info = line.replace('#', '').replace('【經文資訊】', '').strip()
                            if temp_info:
                                meta_info = temp_info

                        # --- 任务 B: 识别并过滤 Header 块 ---
                        if line.startswith('#---'):
                            header_delimiter_count += 1
                            continue

                        if header_delimiter_count > 0 and header_delimiter_count < 2:
                            continue

                            # --- 任务 C: 过滤空行并写入 ---
                        if stripped_line:
                            f_out.write(stripped_line + '\n')

                    # 记录到 CSV
                    csv_writer.writerow([rel_path, meta_info])

                except Exception as e:
                    print(f"\n处理文件出错: {rel_path}, 错误: {e}")

    except IOError as e:
        print(f"无法打开或写入输出文件 (可能文件被占用或权限不足): {e}")
        return

    print("\n处理完成！")
    print(f"合并文档已保存至: {output_txt_path}")
    print(f"信息列表已保存至: {output_csv_path}")


# --- 配置区域 ---
if __name__ == '__main__':
    # 请将此处修改为你的实际文件夹路径
    SOURCE_DIR = r'./database'

    # 输出文件名
    ENCODING_FORMAT = 'gbk'
    OUTPUT_TXT = f'./book/{ENCODING_FORMAT}/sum_{ENCODING_FORMAT}.txt'
    OUTPUT_CSV = f'./book/{ENCODING_FORMAT}/index.csv'
    # 检查源目录是否存在
    if os.path.exists(SOURCE_DIR):
        process_buddhist_scriptures(SOURCE_DIR, OUTPUT_TXT, OUTPUT_CSV, ENCODING_FORMAT)
    else:
        print(f"错误：找不到文件夹 '{SOURCE_DIR}'")
