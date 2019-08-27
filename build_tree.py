import json

import pymysql


def get_cur_node_count(skill_name):
    sql = "SELECT s_count from cqbigdata_skillcount where s_key='%s'" % skill_name
    # sql = "SELECT num from cqbigdata_skill_statistical where skill_name='%s'" % skill_name
    cursor.execute(sql)
    one_data = cursor.fetchone()
    if one_data is not None:
        if one_data[0] is None:
            return 0
        else:
            return one_data[0]
    else:
        return 0


def get_child(node):
    '''获取多个子节点'''
    sql = "select id,trim(LOWER(name)) ,parentid,status from cqbigdata_skilllevels where parentid=%s" % node["id"]
    cursor.execute(sql)
    all_child = cursor.fetchall()
    if all_child is not None:
        for child in all_child:
            # 查询所有根节点
            if child[3] == 1:
                counter = 0
            else:
                counter = get_cur_node_count(child[1])

            child_node = {"id": child[0], "name": child[1], "count": counter, "parentID": child[2], "status": child[3],
                          "child": []}
            node["child"].append(child_node)
            get_child(child_node)


def set_node_count(node):
    if len(node["child"]) > 0:
        # 循环子节点
        for child in node["child"]:
            # 将父节点带入递归
            set_child_node_count(node, child)


def set_child_node_count(node, child):
    if len(child["child"]) > 0:
        for tmp_child in child["child"]:
            if len(tmp_child["child"]) > 0:
                for tmp_child_1 in tmp_child["child"]:
                    set_child_node_count(tmp_child, tmp_child_1)

            if child["status"] == 1:
                child["count"] = child["count"] + tmp_child["count"]

    if node["status"] == 1:
        node["count"] = node["count"] + child["count"]


def get_json():
    try:

        delete_json_sql = "DELETE from cqbigdata_json_result"
        cursor.execute(delete_json_sql)

        cursor.execute("select id,trim(LOWER(name)),STATUS from cqbigdata_skilllevels where parentid=-1")
        # 使用 fetchone() 方法获取单条数据.
        data_list = cursor.fetchall()

        for item in data_list:
            # 对一个根节点使用深度优先算法
            # count = get_cur_node_count(item[1])
            node = {"id": item[0], "name": item[1], "count": 0, "parentID": "-1", "status": item[2], "child": []}
            get_child(node)
            set_node_count(node)
            json_str = json.dumps(node, ensure_ascii=False)
            # sql_command.append()
            cursor.execute("INSERT cqbigdata_json_result(skill_name,json)  VALUES('%s','%s')" % (item[1], json_str))

        # 提交到数据库执行
        db.commit()
        print("执行完毕")


    except Exception as e:
        # 如果发生错误则回滚
        db.rollback()


if __name__ == "__main__":
    # 打开数据库连接
    # db = pymysql.connect("localhost", "root", "root", "hadoopdisk")
    db = pymysql.connect(host="192.168.2.232", port=3306, user="root", passwd="root", db="cqbigdata", charset="utf8mb4")
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()

    get_json()

    # 关闭数据库连接
    db.close()
