# -*- coding: UTF-8 -*-

# 完成推荐需要以下步骤
# 1.读取职位表数据：
# a.jobId,a.salary_min,a.salary_max,a.catalog,a.education,a.job_desc,a.city,a.area,
# 公司表数据：
# b.name,b.company_size,b.company_type,b.address
# 2.读取用户数据：地区、薪资、规模、类型、期望公司
# 3.遍历职位表：按算法模型进行比对打分
# 4.按打分进行排序
# 全部使用Pandas完成
import datetime
import math
import re
import pandas as pd
import multiprocessing
import logging
from logging import handlers
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:root@129.28.172.53:3306/cqbigdata", encoding="utf8")

# 调试模式下是单线程
debug = False

# 每个进程一次处理多少条数据
page_size = 50000

# 默认推荐3条职位
recommend_count = 3

# 名字包含这些内容的职位为0分
unavailable = ["零基础", "0基础", "零基础", "转行", "培训", "实训", "实习", "培养", "学徒",
               "助理", "名企", "委培", "教育", "培训", "实训", "培养", "创智", "汇智", "创想", "育道",
               "达内", "传智播客", "易思哲", "中航云软", "和禹网络", "谢尔科技",
               "睿峰科技", "狮子座科技", "智游网络", "格睿泰思", "华信智原", "百年有为", "亿昇威"]


class Logger(object):
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    def __init__(self, filename, level='info', when='D', backCount=3,
                 fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)
        self.logger.setLevel(self.level_relations.get(level))
        sh = logging.StreamHandler()
        sh.setFormatter(format_str)
        th = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=backCount,
                                               encoding='utf-8')

        th.setFormatter(format_str)
        self.logger.addHandler(sh)
        self.logger.addHandler(th)


class JobRecommend:
    def __init__(self):
        self.pool = multiprocessing.Pool()
        self.process_num = self.get_process_num()
        m = multiprocessing.Manager()
        self.lock = m.Lock()
        self.queue = m.Queue()
        self.log = Logger("recommend.log", level='info')

    def get_user_info_df(self):
        """
            获取用户信息
            :return:
            """
        sql = """
            SELECT actionid,salary_min,salary_max,workingcity,companytype,companyScale,targetCompany,catalog FROM `cqbigdata_userinfo`
            """
        return pd.read_sql(sql, engine)

    def get_job_data_df(self, start_index):
        """
        获取职位数据
        :param start_index:
        :return:
        """
        sql = """
        select a.jobId,a.salary_min,a.salary_max,a.catalog,a.city,a.area,
    b.name,b.company_size,b.company_type,b.address from cqbigdata_job a
    INNER JOIN cqbigdata_company b on a.job_company=b.name LIMIT {0},{1}
        """.format(start_index, page_size)
        df = pd.read_sql(sql, engine)
        return df

    def get_score_extend(self, user_info, job_name, job_salary_min, job_salary_max,
                         job_catalog, job_company_type, job_address, job_company_size):
        """
        根据7个维度打分
        :param user_info:
        :param job_name:
        :param job_salary_min:
        :param job_salary_max:
        :param job_catalog:
        :param job_company_type:
        :param job_address:
        :param job_company_size:
        :return:
        """

        if job_name in unavailable:
            return 0

        score = 0
        # 判定是否是期望公司
        if user_info.targetCompany is not None:
            expect_company_list = user_info.targetCompany.split(",")
            # 是自己期望公司职位+2
            for company in expect_company_list:
                if company in job_name:
                    score += 2
                    break

        # 判定是否在期望薪资范围
        if user_info.salary_min is None or user_info.salary_max is None:
            score += 1
        else:
            if user_info.salary_max < job_salary_min or user_info.salary_min > job_salary_max:
                pass
            else:
                score += 1

        # 判定职位类型：全职 兼职
        if user_info.catalog is None:
            score += 1
        else:
            if user_info.catalog in job_catalog:
                score += 1

        # 判定公司类型：合资 外资
        if user_info.companytype is None:
            score += 1
        else:
            if user_info.companytype in job_company_type:
                score += 1

        # 判定工作地点
        if user_info.workingcity is None:
            score += 1
        else:
            if user_info.workingcity in job_address:
                score += 1

        # 判定公司规模
        if user_info.companyScale is None:
            score += 1
        else:
            if job_company_size is not None:
                # 提取数字
                tmp = re.findall(r"\d+", job_company_size)
                if len(tmp) > 0:
                    size = int(tmp[0])
                    if size >= int(user_info.companyScale):
                        score += 1

        return score

    def get_queue(self):
        """
        从队列中获取计算结果
        :return:
        """
        data = []
        while not self.queue.empty():
            data.append(self.queue.get())

        return data

    def compute_job_score(self, process_no, user_info_df, start_index):
        """
        计算满足条件的df
        首先查找当前范围的职位信息，对当前用户计算得到结果存入队里
        队列的key：{user_id}_task_{i}
        :param q:
        :param process_no:第几号进程
        :param user_info_df:
        :param start_index:查询数据开始位置
        :return:
        """
        job_df = self.get_job_data_df(start_index)
        for user_key, user_info in user_info_df.iterrows():
            queue_key = "{0}_{1}".format(user_info.actionid, process_no)
            score_list = []
            print("工作进程：{0} 正在处理".format(queue_key))
            # 循环ndarry提交执行效率
            jobIds = job_df["jobId"].values
            names = job_df["name"].values
            salary_mins = job_df["salary_min"].values
            salary_maxs = job_df["salary_max"].values
            catalogs = job_df["catalog"].values
            company_types = job_df["company_type"].values
            addresss = job_df["address"].values
            company_sizes = job_df["company_size"].values

            # 当前处理的数据总条数
            cur_slice_count = len(names)

            iter_start = datetime.datetime.now()
            for tmp_index in range(cur_slice_count):
                job_name = names[tmp_index]

                job_salary_min = 0
                if salary_mins[tmp_index] is not None:
                    job_salary_min = int(salary_mins[tmp_index])

                job_salary_max = salary_maxs[tmp_index]
                if salary_maxs[tmp_index] is not None:
                    job_salary_max = int(salary_maxs[tmp_index])

                job_catalog = catalogs[tmp_index]
                job_company_type = company_types[tmp_index]
                job_address = addresss[tmp_index]
                job_company_size = company_sizes[tmp_index]
                score = self.get_score_extend(user_info, job_name, job_salary_min, job_salary_max,
                                              job_catalog, job_company_type, job_address, job_company_size)

                score_list.append({"job_id": jobIds[tmp_index], "score": score})

            iter_end = datetime.datetime.now()
            print("工作进程：{0} 处理完毕,循环耗时(秒)：{1}".format(queue_key, (iter_end - iter_start).seconds))
            self.lock.acquire()
            self.queue.put({queue_key: score_list})
            self.lock.release()

    def filter_data(self, queue_key, all_job_list):
        for job_list in all_job_list:
            if queue_key in job_list.keys():
                return job_list[queue_key]

    def merge_and_sort_df(self, user_info_df):
        """
        从队列里面取出每个用户对应的数据，大概5个，取的key为：{user_id}_task{0-4}
        :param q:
        :param user_info_df:
        :return:
        """

        self.log.logger.info("开始合并各进程计算结果...")
        all_df = None

        try:
            # 获取所有队里中的数据
            all_job = self.get_queue()
            for key, value in user_info_df.iterrows():
                df = None
                for i in range(self.process_num):
                    queue_key = "{0}_{1}".format(value.actionid, i)
                    # 筛选当前key的数据
                    cur_user_job = self.filter_data(queue_key, all_job)
                    if cur_user_job is not None:
                        job_df = pd.DataFrame(cur_user_job)
                        if df is None:
                            df = job_df
                        else:
                            df = df.append(job_df)

                if df is not None:
                    sorted_df = df.sort_values(by="score", ascending=False).head(recommend_count)
                    sorted_df["userid"] = value.actionid
                    if all_df is None:
                        all_df = sorted_df
                    else:
                        all_df = all_df.append(sorted_df)

            if all_df is None:
                self.log.logger.error("未正常执行推荐")
            else:
                all_df = all_df.reset_index(drop=True)
                all_df.to_sql("cqbigdata_recommend_job", engine, if_exists="replace")
                self.log.logger.info("推荐完毕！")
        except Exception as e:
            self.log.logger.error(e)

    def start(self):
        self.log.logger.info("正在获取用户数据...")
        user_info_df = self.get_user_info_df()

        if debug:
            self.compute_job_score(0, user_info_df, 1)
        else:
            self.log.logger.info("创建工作进程开始计算====>>>")
            for i in range(self.process_num):
                start_index = i * page_size + 1
                self.pool.apply_async(func=self.compute_job_score, args=(i, user_info_df, start_index,))

            self.pool.close()
            self.log.logger.info("等待工作进程执行完毕!")
            self.pool.join()

        self.merge_and_sort_df(user_info_df)

    def get_process_num(self):
        """
        获取职位条数，然后根据page_size大小来计算应该有多少个进程
        :return:
        """
        sql = "SELECT max(jobId)-min(jobId)+1 total from cqbigdata_job"
        total = pd.read_sql(sql, engine)
        return math.ceil(float(total.total) / page_size)

    def __getstate__(self):
        self_dict = self.__dict__.copy()
        del self_dict['pool']
        return self_dict

    def __setstate__(self, state):
        self.__dict__.update(state)


if __name__ == '__main__':
    recommend = JobRecommend()
    start = datetime.datetime.now()

    recommend.start()

    end = datetime.datetime.now()
    recommend.log.logger.info("总耗时(秒)：{0}".format((end - start).seconds))
