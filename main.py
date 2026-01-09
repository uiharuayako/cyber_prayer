import os
import sys
import time
import mmap
import json
import platform
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor

# ================= 跨平台配置 =================
IS_WINDOWS = platform.system() == "Windows"

# 默认硬编码路径（仅作后备）
if IS_WINDOWS:
    DEFAULT_TARGET_DIR = r"R:/"
    DEFAULT_TARGET_FILE = r"speed_test_pool.dat"
else:
    DEFAULT_TARGET_DIR = "/mnt/fast_ram"
    DEFAULT_TARGET_FILE = "speed_test_pool.dat"


class RAMDiskManager:
    """处理不同系统的 RAM Disk 初始化"""

    @staticmethod
    def prepare_environment(target_dir, size_gb):
        if IS_WINDOWS:
            return RAMDiskManager._check_windows(target_dir)
        else:
            return RAMDiskManager._setup_linux(target_dir, size_gb)

    @staticmethod
    def _check_windows(target_dir):
        # 检查目录/盘符是否存在
        if not os.path.exists(target_dir):
            print("\n" + "!" * 60)
            print(f"错误: 目标路径不存在 -> {target_dir}")
            print("在 Windows 上，脚本无法自动创建 RAM Disk。")
            print("请使用 'ImDisk Toolkit' 或 'Primo Ramdisk' 创建一个内存盘。")
            print("建议大小: 8GB, 文件系统: NTFS, 盘符: Z:")
            print("!" * 60 + "\n")
            return False
        return True

    @staticmethod
    def _setup_linux(target_dir, size_gb):
        # 检查是否已经挂载
        if os.path.ismount(target_dir):
            print(f"[-] 检测到 Linux 挂载点已存在: {target_dir}")
            return True

        # 尝试创建并挂载
        print(f"[*] 尝试创建并挂载 tmpfs 到 {target_dir} (需要 sudo 权限)...")
        try:
            os.makedirs(target_dir, exist_ok=True)
            # 这里的 size 参数需要加 G 单位
            cmd = ["sudo", "mount", "-t", "tmpfs", "-o", f"size={size_gb}G", "tmpfs", target_dir]
            subprocess.check_call(cmd)
            print(f"[-] 挂载成功。")

            # 修正权限，确保当前用户可写
            user = os.getenv("SUDO_USER") or os.getenv("USER")
            if user:
                subprocess.check_call(["sudo", "chown", "-R", f"{user}:{user}", target_dir])

            return True
        except Exception as e:
            print(f"[!] 挂载失败: {e}")
            print("请手动执行: sudo mount -t tmpfs -o size=8G tmpfs /mnt/fast_ram")
            return False


class WriterThread:
    """写入线程工作类"""

    def __init__(self, thread_id, mm, start_offset, end_offset, source_data, stop_event):
        self.tid = thread_id
        self.mm = mm
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.source_data = source_data
        self.src_len = len(source_data)
        self.stop_event = stop_event

        self.write_count = 0
        self.total_bytes = 0

    def run(self):
        cursor = self.start_offset
        # 局部变量提速
        mm = self.mm
        src_data = self.source_data
        src_len = self.src_len
        start_off = self.start_offset
        end_off = self.end_offset

        try:
            # 只有在未收到停止信号时才继续下一轮完整写入
            while not self.stop_event.is_set():
                end_pos = cursor + src_len

                if end_pos <= end_off:
                    # 直接写入
                    mm[cursor:end_pos] = src_data
                    cursor = end_pos
                else:
                    # 回滚逻辑 (Wrap around)
                    remaining = end_off - cursor
                    if remaining > 0:
                        mm[cursor:end_off] = src_data[:remaining]

                    overflow = src_len - remaining
                    new_cursor = start_off + overflow
                    mm[start_off:new_cursor] = src_data[remaining:]
                    cursor = new_cursor

                # 完成一次完整写入后更新统计
                self.write_count += 1
                self.total_bytes += src_len

        except Exception as e:
            print(f"[Thread {self.tid}] Error: {e}")


def load_config():
    """读取同目录下的 config.json"""
    base_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_path, "config.json")

    if not os.path.exists(config_path):
        print(f"[!] 错误: 找不到配置文件: {config_path}")
        print(f"请确保 config.json 位于脚本同一目录下。")
        sys.exit(1)

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[!] 配置文件读取失败: {e}")
        sys.exit(1)


def main():
    # 1. 读取配置
    cfg = load_config()

    # 解析参数 (带默认值回落逻辑)
    source_file = cfg.get("source_file", "")

    target_dir = cfg.get("target_dir", "AUTO")
    if target_dir in ["AUTO", "", None]:
        target_dir = DEFAULT_TARGET_DIR

    filename = cfg.get("target_filename", DEFAULT_TARGET_FILE)

    threads = cfg.get("threads", 0)
    if threads <= 0:
        threads = os.cpu_count() or 4

    pool_size = cfg.get("pool_size_gb", 6)
    interval = cfg.get("stats_interval_sec", 1)

    # 组合路径
    target_full_path = os.path.join(target_dir, filename)

    # 版本状态
    is_free_threaded = sys.version.find("free-threaded") != -1 or sys.version_info >= (3, 13)
    gil_status = "ENABLED (Standard)" if not is_free_threaded else "DISABLED (Free-Threaded)"

    print(f"=== RAM Disk Speed Test (Config Mode) ===")
    print(f"Python Ver : {platform.python_version()} [{gil_status}]")
    print(f"配置文件   : config.json")
    print(f"源文件     : {source_file}")
    print(f"目标地     : {target_full_path}")
    print(f"线程数     : {threads}")
    print(f"池大小     : {pool_size} GB")
    print("============================================")

    # 2. 环境准备
    if not RAMDiskManager.prepare_environment(target_dir, pool_size + 1):
        return

    # 3. 读取源文件
    if not os.path.exists(source_file):
        print(f"[!] 错误: 源文件 '{source_file}' 不存在")
        print("请修改 config.json 中的 'source_file' 路径。")
        return

    print("[-] 正在加载源文件到内存...")
    with open(source_file, 'rb') as f:
        source_data = f.read()
    print(f"[-] 源文件大小: {len(source_data) / 1024 / 1024:.2f} MB")

    # 4. 预分配文件
    pool_bytes = pool_size * 1024 * 1024 * 1024
    print(f"[-] 正在预分配 {pool_size} GB 空间...")
    try:
        with open(target_full_path, 'wb') as f:
            f.truncate(pool_bytes)
    except OSError as e:
        print(f"[!] 创建测试文件失败: {e}")
        print("Windows用户请确认盘符正确且有写入权限。")
        return

    # 5. 内存映射
    try:
        f_target = open(target_full_path, 'r+b')
        mm = mmap.mmap(f_target.fileno(), 0)
    except Exception as e:
        print(f"[!] 内存映射失败: {e}")
        return

    # 6. 启动线程
    stop_event = threading.Event()
    workers = []
    chunk_size = pool_bytes // threads

    print(f"[-] 启动测试引擎 (统计间隔: {interval}s)...")
    executor = ThreadPoolExecutor(max_workers=threads)

    for i in range(threads):
        start = i * chunk_size
        end = start + chunk_size
        w = WriterThread(i, mm, start, end, source_data, stop_event)
        workers.append(w)
        executor.submit(w.run)

    # 7. 监控显示
    print("\n测试正在运行。按 Ctrl+C 平滑停止（完成当前写入后再停止）。\n")
    header = f"{'Time':<10} | {'Speed (GB/s)':<14} | {'IOPS (iter/s)':<15} | {'Total Writes':<14} | {'Total Data (GB)':<15}"
    print(header)
    print("-" * len(header))

    start_test_time = time.perf_counter()
    last_total_bytes = 0
    last_total_count = 0
    last_time = start_test_time

    try:
        while True:
            time.sleep(interval)
            now = time.perf_counter()

            cur_bytes = sum(w.total_bytes for w in workers)
            cur_count = sum(w.write_count for w in workers)

            diff_bytes = cur_bytes - last_total_bytes
            diff_count = cur_count - last_total_count
            diff_time = now - last_time

            if diff_time <= 0: diff_time = 0.001

            speed_gb = (diff_bytes / 1024 ** 3) / diff_time
            iops = diff_count / diff_time
            total_gb_display = cur_bytes / 1024 ** 3

            t_str = time.strftime("%H:%M:%S")
            print(f"{t_str:<10} | {speed_gb:<14.2f} | {iops:<15.1f} | {cur_count:<14,} | {total_gb_display:<15.2f}")

            last_total_bytes = cur_bytes
            last_total_count = cur_count
            last_time = now

    except KeyboardInterrupt:
        print("\n\n[*] 接收到停止信号 (Ctrl+C)...")

        # 通知线程在完成当前工作后停止
        stop_event.set()

        print("[*] 等待所有线程完成当前写入操作 (Graceful Shutdown)...")
        executor.shutdown(wait=True)

        # 最终统计
        end_time = time.perf_counter()
        total_time = end_time - start_test_time
        if total_time <= 0: total_time = 0.001

        final_bytes = sum(w.total_bytes for w in workers)
        final_count = sum(w.write_count for w in workers)
        final_gb = final_bytes / 1024 ** 3

        avg_speed = final_gb / total_time
        avg_iops = final_count / total_time

        print("\n" + "=" * 50)
        print("              测试结果汇总")
        print("=" * 50)
        print(f"运行时间     : {total_time:.2f} 秒")
        print(f"总写入次数   : {final_count:,} 次 (完整文件)")
        print(f"总写入数据   : {final_gb:.2f} GB")
        print(f"平均速度     : {avg_speed:.2f} GB/s")
        print(f"平均 IOPS    : {avg_iops:.1f} iter/s")
        print("=" * 50)

        # 资源清理
        mm.close()
        f_target.close()
        print("[-] 资源已释放，程序退出。")


if __name__ == "__main__":
    main()