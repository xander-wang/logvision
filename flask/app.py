# coding=utf-8
import ast
import time
from kafka import KafkaConsumer
import redis
import requests

from threading import Lock, Thread
from flask import Flask, render_template, session, request
from flask_socketio import SocketIO, emit

async_mode = None
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode=async_mode)

thread = None
thread_lock = Lock()

# 配置项目
time_interval = 1
kafka_bootstrap_servers = "10.0.0.222:9092"
redis_con_pool = redis.ConnectionPool(host='10.0.0.222', port=6379, decode_responses=True)
# baidu map app key, replace it with your own key
map_ak = ''


# 页面路由与对应页面的ws接口
# 系统时间
@socketio.on('connect', namespace='/sys_time')
def sys_time():
    def loop():
        while True:
            socketio.sleep(time_interval)
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            socketio.emit('sys_time',
                          {'data': current_time},
                          namespace='/sys_time')

    socketio.start_background_task(target=loop)


# 欢迎页面
@app.route('/')
@app.route('/welcome')
def welcome():
    return render_template('index.html', async_mode=socketio.async_mode)


# 实时日志流
@socketio.on('connect', namespace='/log_stream')
def log_stream():
    def loop():
        socketio.sleep(time_interval)
        consumer = KafkaConsumer("raw_log", bootstrap_servers=kafka_bootstrap_servers)
        cache = ""
        for msg in consumer:
            cache += bytes.decode(msg.value) + "\n"
            if len(cache.split("\n")) == 25:
                socketio.emit('log_stream',
                              {'data': cache},
                              namespace='/log_stream')
                cache = ""

    socketio.start_background_task(target=loop)


# 实时日志分析页面
@app.route('/analysis')
def analysis():
    return render_template('analysis.html', async_mode=socketio.async_mode)


# 实时计数器
@socketio.on('connect', namespace='/count_board')
def count_board():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = redis_con.zrange("statcode", 0, 40, withscores=True)

            # 总请求数（日志行数）
            host_count = redis_con.zscore("line", "count")

            # 成功请求数（状态码属于normal的个数）
            normal = ["200", "201", "202", "203", "204", "205", "206", "207"]
            success_count = 0
            for i in res:
                if i[0] in normal:
                    success_count += int(i[1])

            # 其他请求数（其他状态码个数）
            other_count = 0
            for i in res:
                other_count += int(i[1])
            other_count -= success_count

            # 访客数（不同的IP个数）
            visitor_count = redis_con.zcard("host")

            # 资源数（不同的url个数）
            url_count = redis_con.zcard("url")

            # 流量大小（bytes的和，MB）
            traffic_sum = int(redis_con.zscore("traffic", "sum"))

            # 日志大小（MB）
            log_size = int(redis_con.zscore("size", "sum"))

            socketio.emit('count_board',
                          {'host_count': host_count,
                           'success_count': success_count,
                           'other_count': other_count,
                           'visitor_count': visitor_count,
                           'url_count': url_count,
                           'traffic_sum': traffic_sum,
                           'log_size': log_size},
                          namespace='/count_board')

    socketio.start_background_task(target=loop)


# 实时热门位置
@socketio.on('connect', namespace='/hot_geo')
def hot_geo():
    def loop():
        while True:
            socketio.sleep(2)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = redis_con.zrevrange("host", 0, 50, withscores=True)
            data = []

            for i in res:
                # 调用接口获取地理坐标
                req = requests.get("http://api.map.baidu.com/location/ip",
                                   {'ak': map_ak,
                                    'ip': i[0],
                                    'coor': 'bd09ll'})
                body = eval(req.text)

                # 仅显示境内定位
                if body['status'] == 0:
                    coor_x = body['content']['point']['x']
                    coor_y = body['content']['point']['y']

                    data.append({"name": i[0], "value": [coor_x, coor_y, i[1]]})

            socketio.emit('hot_geo',
                          {'data': data},
                          namespace='/hot_geo')

    socketio.start_background_task(target=loop)


# 实时热门资源排名
@socketio.on('connect', namespace='/hot_url')
def hot_url():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = redis_con.zrevrange("url", 0, 9, withscores=True)
            data = []
            no = 1

            for i in res:
                data.append({"no": no, "url": i[0], "count": i[1]})
                no += 1

            socketio.emit('hot_url',
                          {'data': data},
                          namespace='/hot_url')

    socketio.start_background_task(target=loop)


# 实时热门IP排名
@socketio.on('connect', namespace='/hot_ip')
def hot_ip():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = redis_con.zrevrange("host", 0, 13, withscores=True)
            data = []
            no = 1

            for i in res:
                # 调用接口获取地理坐标
                req = requests.get("http://api.map.baidu.com/location/ip",
                                   {'ak': map_ak,
                                    'ip': i[0],
                                    'coor': 'bd09ll'})
                body = eval(req.text)

                # 仅显示境内定位
                if body['status'] == 0:
                    address = body['content']['address']

                    data.append({"no": no, "ip": i[0], "address": address, "count": i[1]})
                    no += 1

            socketio.emit('hot_ip',
                          {'data': data},
                          namespace='/hot_ip')

    socketio.start_background_task(target=loop)


# 实时状态码比例
@socketio.on('connect', namespace='/status_code_pie')
def status_code_pie():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = redis_con.zrevrange("statcode", 0, 100, withscores=True)
            data = []
            legend = []

            for i in res:
                if i[0] != 'foo':
                    data.append({"value": i[1], "name": i[0]})
                    legend.append(i[0])

            socketio.emit('status_code_pie',
                          {'legend': legend, 'data': data},
                          namespace='/status_code_pie')

    socketio.start_background_task(target=loop)


# 实时请求方式比例
@socketio.on('connect', namespace='/req_method_pie')
def req_method_pie():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = redis_con.zrevrange("reqmt", 0, 100, withscores=True)
            data = []
            legend = []

            for i in res:
                if i[0] != 'foo':
                    data.append({"value": i[1], "name": i[0]})
                    legend.append(i[0])

            socketio.emit('req_method_pie',
                          {'legend': legend, 'data': data},
                          namespace='/req_method_pie')

    socketio.start_background_task(target=loop)


# 实时请求计数（按时间顺序）
@socketio.on('connect', namespace='/req_count_timeline')
def req_count_timeline():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = dict(redis_con.zrange("datetime", 0, 10000000, withscores=True))
            data = []
            date = []

            # 按时间排序
            for i in sorted(res):
                datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(i) / 1000))
                data.append(res[i])
                date.append(datetime)

            socketio.emit('req_count_timeline',
                          {"data": data, "date": date},
                          namespace='/req_count_timeline')

    socketio.start_background_task(target=loop)


# IP请求数排序
@socketio.on('connect', namespace='/ip_ranking')
def timestamp_count_timeline():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = redis_con.zrevrange("host", 0, 50, withscores=True)
            ip = []
            count = []

            for i in res:
                ip.append(i[0])
                count.append(i[1])

            socketio.emit('ip_ranking',
                          {"ip": ip, "count": count},
                          namespace='/ip_ranking')

    socketio.start_background_task(target=loop)


@app.route('/id')
def id():
    return render_template("id.html", async_mode=socketio.async_mode)


# 异常请求计数
@socketio.on('connect', namespace='/bad_count')
def bad_count():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = int(redis_con.zscore("bad", "bad"))

            socketio.emit('bad_count',
                          {"data": res},
                          namespace='/bad_count')

    socketio.start_background_task(target=loop)


# 正常请求计数
@socketio.on('connect', namespace='/good_count')
def bad_count():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            res = int(redis_con.zscore("good", "good"))

            socketio.emit('good_count',
                          {"data": res},
                          namespace='/good_count')

    socketio.start_background_task(target=loop)


# 正常请求地理标记
@socketio.on('connect', namespace='/good_geo')
def good_geo():
    def loop():
        while True:
            socketio.sleep(time_interval)
            consumer = KafkaConsumer("good_result", bootstrap_servers=kafka_bootstrap_servers)
            data = []

            for msg in consumer:
                result = ast.literal_eval(bytes.decode(msg.value))
                for record in result:
                    if record['host'] != "foo":
                        # 调用接口获取地理坐标
                        req = requests.get("http://api.map.baidu.com/location/ip",
                                           {'ak': map_ak,
                                            'ip': record['host'],
                                            'coor': 'bd09ll'})
                        body = eval(req.text)
                        # 仅显示境内定位
                        if body['status'] == 0:
                            coor_x = body['content']['point']['x']
                            coor_y = body['content']['point']['y']
                            datetime = time.strftime("%Y-%m-%d %H:%M:%S",
                                                     time.localtime(int(record['timestamp']) / 1000))

                            data.append({"name": record['host'], "value": [coor_x, coor_y,
                                                                           record['url'],
                                                                           datetime,
                                                                           record['req_method'],
                                                                           record['protocol'],
                                                                           record['status_code']]})
                            socketio.emit('good_geo',
                                          {"data": data},
                                          namespace='/good_geo')

    socketio.start_background_task(target=loop)


# 异常请求地理标记
@socketio.on('connect', namespace='/bad_geo')
def bad_geo():
    def loop():
        while True:
            socketio.sleep(time_interval)
            consumer = KafkaConsumer("bad_result", bootstrap_servers=kafka_bootstrap_servers)
            data = []

            for msg in consumer:
                result = ast.literal_eval(bytes.decode(msg.value))
                for record in result:
                    if record['host'] != "foo":
                        # 调用接口获取地理坐标
                        req = requests.get("http://api.map.baidu.com/location/ip",
                                           {'ak': map_ak,
                                            'ip': record['host'],
                                            'coor': 'bd09ll'})
                        body = eval(req.text)
                        # 仅显示境内定位
                        if body['status'] == 0:
                            coor_x = body['content']['point']['x']
                            coor_y = body['content']['point']['y']
                            datetime = time.strftime("%Y-%m-%d %H:%M:%S",
                                                     time.localtime(int(record['timestamp']) / 1000))

                            data.append({"name": record['host'], "value": [coor_x, coor_y,
                                                                           record['url'],
                                                                           datetime,
                                                                           record['req_method'],
                                                                           record['protocol'],
                                                                           record['status_code']]})
                            socketio.emit('bad_geo',
                                          {"data": data},
                                          namespace='/bad_geo')

    socketio.start_background_task(target=loop)


# 实时入侵分类计数（按时间顺序）
@socketio.on('connect', namespace='/url_cate_count_timeline')
def url_cate_count_timeline():
    def loop():
        while True:
            socketio.sleep(time_interval)
            redis_con = redis.Redis(connection_pool=redis_con_pool)
            good_res = dict(redis_con.zrange("goodts", 0, 10000000, withscores=True))
            bad_res = dict(redis_con.zrange("badts", 0, 10000000, withscores=True))

            # 求正常和异常结果的时间戳的并集，并排序。再生成对应的正常和异常计数
            date = []
            date_ts = []
            good_date = []
            bad_date = []

            good_data = []
            bad_data = []
            # 求并集并排序
            for i in good_res:
                good_date.append(i)
            for j in bad_res:
                bad_date.append(j)
            for k in sorted(list(set(good_date) | set(bad_date))):
                date_ts.append(k)

            # 生成对应的计数
            for t in date_ts:
                if t in good_res:
                    good_data.append(good_res[t])
                else:
                    good_data.append(0)
                if t in bad_res:
                    bad_data.append(bad_res[t])
                else:
                    bad_data.append(0)
            # 时间戳转字符串
            for ts in date_ts:
                date.append(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts) / 1000)))

            socketio.emit('url_cate_count_timeline',
                          {"date": date, "good_data": good_data, "bad_data": bad_data},
                          namespace='/url_cate_count_timeline')

    socketio.start_background_task(target=loop)


# 实时异常请求概览
@socketio.on('connect', namespace='/bad_detail')
def bad_detail():
    def loop():
        while True:
            socketio.sleep(time_interval)
            consumer = KafkaConsumer("bad_result", bootstrap_servers=kafka_bootstrap_servers)
            data = []

            for msg in consumer:
                result = ast.literal_eval(bytes.decode(msg.value))
                for record in result:
                    if record['host'] != "foo":
                        # 调用接口获取地理坐标
                        req = requests.get("http://api.map.baidu.com/location/ip",
                                           {'ak': map_ak,
                                            'ip': record['host'],
                                            'coor': 'bd09ll'})
                        body = eval(req.text)
                        # 仅显示境内定位
                        if body['status'] == 0:
                            address = body['content']['address']

                            datetime = time.strftime("%Y-%m-%d %H:%M:%S",
                                                     time.localtime(int(record['timestamp']) / 1000))

                            data.append({"host": record['host'], "address": address, "url": record['url'],
                                         "datetime": datetime, "req_method": record['req_method'],
                                         "protocol": record['protocol'], "status_code": record['status_code'],
                                         "pred": record['prediction'], 'prob': record['probability']['values']})

                            socketio.emit('bad_detail',
                                          {"data": data},
                                          namespace='/bad_detail')
    socketio.start_background_task(target=loop)


# 实时正常请求概览
@socketio.on('connect', namespace='/good_detail')
def good_detail():
    def loop():
        while True:
            socketio.sleep(time_interval)
            consumer = KafkaConsumer("good_result", bootstrap_servers=kafka_bootstrap_servers)
            data = []

            for msg in consumer:
                result = ast.literal_eval(bytes.decode(msg.value))
                for record in result:
                    if record['host'] != "foo":
                        # 调用接口获取地理坐标
                        req = requests.get("http://api.map.baidu.com/location/ip",
                                           {'ak': map_ak,
                                            'ip': record['host'],
                                            'coor': 'bd09ll'})
                        body = eval(req.text)
                        # 仅显示境内定位
                        if body['status'] == 0:
                            address = body['content']['address']

                            datetime = time.strftime("%Y-%m-%d %H:%M:%S",
                                                     time.localtime(int(record['timestamp']) / 1000))

                            data.append({"host": record['host'], "address": address, "url": record['url'],
                                         "datetime": datetime, "req_method": record['req_method'],
                                         "protocol": record['protocol'], "status_code": record['status_code'],
                                         "pred": record['prediction'], 'prob': record['probability']['values']})

                            socketio.emit('good_detail',
                                          {"data": data},
                                          namespace='/good_detail')
    socketio.start_background_task(target=loop)


@app.route('/about')
def about():
    return render_template("about.html", async_mode=socketio.async_mode)


if __name__ == '__main__':
    socketio.run(app, host="0.0.0.0", port=5000, debug=True)
