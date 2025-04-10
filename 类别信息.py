import pandas as pd
import os

# 定义路径
csv_file = r"D:\lab\social_online\MFAN-main\MFAN\dataset\pheme\content.csv"  # 输入 CSV 文件路径
output_csv_path = r"D:\lab\social_online\MFAN-main\MFAN\dataset\pheme\content_type.csv"  # 输出文件路径
root_folder = r"D:\web_download\phemernrdataset (1)\pheme-rnr-dataset"  # 根文件夹路径
output_csv_path2 = r"D:\lab\social_online\MFAN-main\MFAN\dataset\pheme\content_type_num.csv"
def classify_posts_and_count(csv_path, root_dir, output_path):
    # 读取 CSV 文件
    df = pd.read_csv(csv_path, header=None)  # 无标题行的 CSV
    print("成功加载 CSV 文件...")

    # 确保 CSV 文件有足够的列
    if df.shape[1] < 2:
        print("错误：CSV 文件至少需要两列数据，第二列为帖子 ID！")
        return

    # 提取帖子 ID 列并清理空格
    post_ids = df.iloc[:, 1].astype(str).str.strip()  # 第二列为帖子 ID

    # 构造帖子 ID 到类别的映射字典
    post_id_to_category = {}

    # 遍历根目录下的所有事件（如 charliehebdo，sydneysiege）
    print("开始遍历文件夹...")
    for i, category_folder in enumerate(sorted(os.listdir(root_dir))):
        category_path = os.path.join(root_dir, category_folder)

        # 确保是有效的目录
        if not os.path.isdir(category_path):
            print(f"跳过非目录：{category_folder}")
            continue

        print(f"处理事件类别文件夹：{category_folder} -> 编号 {i + 1}")

        # 遍历类别下的所有文件夹（如 rumours 与 non-rumours）
        for subfolder in os.listdir(category_path):
            subfolder_path = os.path.join(category_path, subfolder)

            if not os.path.isdir(subfolder_path):
                print(f"跳过非目录：{subfolder}")
                continue

            # 遍历子文件夹中所有帖子 ID 文件夹
            for post_id_folder in os.listdir(subfolder_path):
                post_folder_path = os.path.join(subfolder_path, post_id_folder)

                if os.path.isdir(post_folder_path):
                    # 添加帖子 ID 到类别的映射
                    post_id_key = post_id_folder.strip()  # 去掉空格
                    post_id_to_category[post_id_key] = i + 1  # 用数字表示类别编号

    print("文件夹遍历完成！")
    print(f"找到的帖子 ID 总数：{len(post_id_to_category)}")
    print(f"映射样例：{list(post_id_to_category.items())[:10]}")  # 打印部分映射数据

    # 为 CSV 数据匹配类别
    categories = post_ids.map(post_id_to_category).fillna(0).astype(int)  # 未匹配到的设为类别 `0`
    df['类别'] = categories  # 将分类结果增加到新列中

    # 检查未分类的帖子 ID
    total_missing = (categories == 0).sum()
    if total_missing > 0:
        print(f"警告：有 {total_missing} 个帖子 ID 未找到对应类别！")
        missing_ids = post_ids[categories == 0].tolist()
        print(f"未分类的帖子 ID（部分）：{missing_ids[:10]}")

    # 保存处理后的 CSV
    df.to_csv(output_path, index=False, header=False, encoding='utf-8-sig')
    print(f"处理完成！分类添加后的 CSV 文件已保存至：{output_path}")

    # 分类统计：统计每个类别的帖子个数（包括类别编号为 0 的，即未匹配到的）
    category_counts = df['类别'].value_counts().sort_index()
    print("\n各类别的帖子数量统计：")
    for category, count in category_counts.items():
        if category == 0:
            print(f"未分类：{count} 条")
        else:
            print(f"类别 {category}：{count} 条")




def add_sequential_numbers(input_csv_path, output_csv_path):
    """
    为 CSV 文件左侧添加一个数字序列列，从第 2 行（数据部分）开始编号。
    第 1 行不参与编号（作为标题行）。

    参数：
    - input_csv_path: 输入 CSV 文件的路径
    - output_csv_path: 输出 CSV 文件的路径
    """
    # 读取 CSV 文件，假定第一行是标题
    df = pd.read_csv(input_csv_path, header=None)  # 没有标题行
    print("成功加载 CSV 文件...")

    # 提取标题行并分离数据部分
    header = df.iloc[0]  # 第一行是标题行
    data = df.iloc[1:]  # 从第二行开始是数据部分
    data.reset_index(drop=True, inplace=True)  # 重置索引

    # 添加序号列，从 1 开始编号
    data.insert(0, '序号', range(1, len(data) + 1))  # 插入第一列为“序号”列，从 1 开始

    # 将标题行与数据重新组装
    header_list = ['序号'] + header.tolist()  # 在标题行中插入“序号”
    header_df = pd.DataFrame([header_list], columns=data.columns)  # 创建仅包含标题的 DataFrame

    # 使用 pd.concat 将标题行和数据部分合并
    result_df = pd.concat([header_df, data], ignore_index=True)

    # 保存到输出文件
    result_df.to_csv(output_csv_path, index=False, header=False, encoding='utf-8-sig')
    print(f"处理完成！新增序号后的 CSV 文件已保存至：{output_csv_path}")


def count_labels_by_type(csv_file_path):
    """
    统计 CSV 文件中每种 `type` 下各个 `label` 的数量。

    参数：
    - csv_file_path: 输入 CSV 文件的路径

    返回：
    打印每种 `type` 下的 `label` 的统计数量。
    """
    # 加载 CSV 文件
    df = pd.read_csv(csv_file_path)
    print("成功加载 CSV 文件...")

    # 确保标题行包含 num, imgnum, mid, text, label, type
    required_columns = {'num', 'imgnum', 'mid', 'text', 'label', 'type'}
    if not required_columns.issubset(df.columns):
        print(f"错误：CSV 文件缺少必要列 {required_columns - set(df.columns)}")
        return

    # 按 type 和 label 分组并统计数量
    grouped_counts = df.groupby(['type', 'label']).size().reset_index(name='count')

    # 打印统计结果
    print("\n各 type 下的 label 数量统计：")
    for type_value, group in grouped_counts.groupby('type'):
        print(f"\nType: {type_value}")
        for _, row in group.iterrows():
            print(f"  Label {row['label']}: {row['count']} 条")
# 执行分类和统计函数
# classify_posts_and_count(csv_file, root_folder, output_csv_path)
# add_sequential_numbers(output_csv_path,output_csv_path2)
count_labels_by_type(output_csv_path2)