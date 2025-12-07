import os
import json
import psycopg2
from psycopg2.extras import execute_batch

# ======================== 数据库配置 ========================
DB_CONFIG = {
    "host": "******",       # 数据库主机
    "port": "******",            # 数据库端口
    "dbname": "cs209a_stackoverflow",  # 目标数据库名
    "user": "******",        # 数据库用户名
    "password": "******"  # 数据库密码
}
# ===========================================================

class StackOverflowDataImporter:
    def __init__(self):
        self.conn = None
        self.cur = None
        self.connect_db()

    def connect_db(self):
        """连接PostgreSQL数据库"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            print("数据库连接成功！")
        except Exception as e:
            raise RuntimeError(f"数据库连接失败：{str(e)}")

    def close_db(self):
        """关闭数据库连接"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("数据库连接已关闭")

    def upsert_user(self, user_data):
        """插入/更新用户（表名改为users）"""
        sql = """
            INSERT INTO users (user_id, account_id, reputation, user_type, accept_rate, profile_image, display_name, link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET reputation = EXCLUDED.reputation,
                accept_rate = EXCLUDED.accept_rate,
                profile_image = EXCLUDED.profile_image,
                display_name = EXCLUDED.display_name,
                link = EXCLUDED.link;
        """
        params = (
            user_data["user_id"],
            user_data.get("account_id"),
            user_data["reputation"],
            user_data["user_type"],
            user_data.get("accept_rate"),
            user_data.get("profile_image"),
            user_data["display_name"],
            user_data.get("link")
        )
        self.cur.execute(sql, params)

    def insert_question(self, question_data, owner_user_id):
        """插入问题"""
        sql = """
            INSERT INTO questions (question_id, title, body, owner_user_id, is_answered, view_count, answer_count, score,
                                 accepted_answer_id, creation_date, last_edit_date, last_activity_date, protected_date,
                                 content_license, link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (question_id) DO NOTHING;
        """
        params = (
            question_data["question_id"],
            question_data["title"],
            question_data["body"],
            owner_user_id,
            question_data["is_answered"],
            question_data["view_count"],
            question_data["answer_count"],
            question_data["score"],
            question_data.get("accepted_answer_id"),
            question_data["creation_date"],
            question_data.get("last_edit_date"),
            question_data["last_activity_date"],
            question_data.get("protected_date"),
            question_data["content_license"],
            question_data["link"]
        )
        self.cur.execute(sql, params)

    def insert_question_tags(self, question_id, tags):
        """插入问题标签"""
        sql = """
            INSERT INTO question_tags (question_id, tag)
            VALUES (%s, %s)
            ON CONFLICT (question_id, tag) DO NOTHING;
        """
        params_list = [(question_id, tag) for tag in tags]
        execute_batch(self.cur, sql, params_list, page_size=100)  # 批量插入优化

    def insert_answers(self, answers_data, question_id):
        """插入回答"""
        sql = """
            INSERT INTO answers (answer_id, question_id, owner_user_id, body, is_accepted, score,
                               creation_date, last_edit_date, last_activity_date, content_license)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (answer_id) DO NOTHING;
        """
        params_list = []
        for answer in answers_data:
            params = (
                answer["answer_id"],
                question_id,
                answer["owner"]["user_id"],
                answer["body"],
                answer["is_accepted"],
                answer["score"],
                answer["creation_date"],
                answer.get("last_edit_date"),
                answer["last_activity_date"],
                answer["content_license"]
            )
            params_list.append(params)
        execute_batch(self.cur, sql, params_list, page_size=100)

    def insert_comments(self, comments_data, post_type):
        """插入评论"""
        # 转换post_type：question→Q，answer→A
        post_type_char = "Q" if post_type == "question" else "A"
        
        sql = """
            INSERT INTO comments (comment_id, post_type, post_id, owner_user_id, reply_to_user_id,
                                edited, score, creation_date, content_license)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (comment_id) DO NOTHING;
        """
        params_list = []
        for comment in comments_data:
            reply_to_user_id = comment.get("reply_to_user", {}).get("user_id")
            params = (
                comment["comment_id"],
                post_type_char,
                comment["post_id"],
                comment["owner"]["user_id"],
                reply_to_user_id,
                comment["edited"],
                comment["score"],
                comment["creation_date"],
                comment["content_license"]
            )
            params_list.append(params)
        execute_batch(self.cur, sql, params_list, page_size=100)

    def process_single_json(self, json_file_path):
        """处理单个JSON文件"""
        try:
            # 读取JSON文件
            with open(json_file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            question_data = data["question"]
            answers_data = data["answers"]
            question_comments = data["question_comments"]
            answer_comments = data["answer_comments"]

            # 1. 收集所有用户（JSON字符串去重，避免重复插入）
            all_users = set()
            # 问题作者
            all_users.add(json.dumps(question_data["owner"], sort_keys=True))
            # 回答作者
            for answer in answers_data:
                all_users.add(json.dumps(answer["owner"], sort_keys=True))
            # 问题评论作者+回复对象
            for comment in question_comments:
                all_users.add(json.dumps(comment["owner"], sort_keys=True))
                if "reply_to_user" in comment and comment["reply_to_user"]:
                    all_users.add(json.dumps(comment["reply_to_user"], sort_keys=True))
            for comments in answer_comments.values():
                for comment in comments:
                    all_users.add(json.dumps(comment["owner"], sort_keys=True))
                    if "reply_to_user" in comment and comment["reply_to_user"]:
                        all_users.add(json.dumps(comment["reply_to_user"], sort_keys=True))

            # 插入/更新用户
            for user_str in all_users:
                user = json.loads(user_str)
                self.upsert_user(user)

            # 2. 插入问题
            self.insert_question(question_data, question_data["owner"]["user_id"])

            # 3. 插入问题标签
            self.insert_question_tags(question_data["question_id"], question_data["tags"])

            # 4. 插入回答
            self.insert_answers(answers_data, question_data["question_id"])

            # 5. 插入问题评论（post_type=Q）
            self.insert_comments(question_comments, "question")

            # 6. 插入回答评论（post_type=A）
            all_answer_comments = []
            for comments in answer_comments.values():
                all_answer_comments.extend(comments)
            self.insert_comments(all_answer_comments, "answer")

            # 提交事务
            self.conn.commit()
            print(f"成功处理文件：{os.path.basename(json_file_path)}")

        except Exception as e:
            # 出错回滚
            self.conn.rollback()
            print(f"处理文件失败 {os.path.basename(json_file_path)}：{str(e)}")

    def batch_import(self, json_dir_path):
        """批量处理指定目录下的所有JSON文件"""
        # 校验目录是否存在
        if not os.path.exists(json_dir_path):
            raise RuntimeError(f"目录不存在：{json_dir_path}")
        
        # 筛选所有.json文件
        json_files = [f for f in os.listdir(json_dir_path) if f.endswith(".json")]
        if not json_files:
            raise RuntimeError(f"目录 {json_dir_path} 下无JSON文件")

        print(f"共找到 {len(json_files)} 个JSON文件，开始批量导入...")
        # 遍历处理每个文件
        for json_file in json_files:
            file_path = os.path.join(json_dir_path, json_file)
            self.process_single_json(file_path)

        print("所有JSON文件导入完成！")

if __name__ == "__main__":
    # ====================== JSON路径配置 ======================
    JSON_DIR = r"******"
    # =========================================================

    importer = None
    try:
        importer = StackOverflowDataImporter()
        importer.batch_import(JSON_DIR)
    except Exception as e:
        print(f"\n导入过程出错：{str(e)}")
    finally:
        if importer:
            importer.close_db()