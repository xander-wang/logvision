from flask import Flask, render_template
from flask_socketio import SocketIO
import redis
from threading import Lock
import requests
import controller
import time

async_mode = None
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode=async_mode)

thread = None
thread_lock = Lock()


def background_thread():
    sinker = controller.Sinker("192.168.83.33:9092", "192.168.83.33:9092", 6379).run()
    count = 0
    while True:
        socketio.sleep(5)
        count += 1

        # Get data from Redis
        # Counting board & redis data entry - sorted set
        redis_connection = redis.Redis(host="127.0.0.1", port=6379, db=0)
        total_req = redis_connection.zscore("count-total-host", "total-req")
        visitor = redis_connection.zcard("count-host")
        request_type = (redis_connection.zscore("count-statuscode", 200),
                        redis_connection.zscore("count-statuscode", 404))
        traffic = redis_connection.zscore("count-bytes", "traffic")
        log_size = redis_connection.zscore("count-char", "log-size")
        intrusion_count = redis_connection.zscore("count-intrusion", "intrusion-count")
        normal_count = redis_connection.zscore("count-normal", "normal-count")
        host_ranking = redis_connection.zrevrange("count-host", 0, 10, withscores=True)
        req_ranking = redis_connection.zrevrange("count-req-url", 0, 10, withscores=True)
        intrusion_datetime_count_raw = redis_connection.zrange("count-intrusion-datetime", 0, -1, withscores=True)
        normal_datetime_count_raw = redis_connection.zrange("count-normal-datetime", 0, -1, withscores=True)
        statuscode_raw = redis_connection.zrange("count-statuscode", 0, -1, withscores=True)
        req_method_raw = redis_connection.zrange("count-req-method", 0, -1, withscores=True)
        datetime_raw = redis_connection.zrange("count-datetime", 0, -1, withscores=True)

        # Intrusion & Normal datetime statistics
        intrusion_container = {}
        normal_container = {}
        i_n_intrusion_count = []
        i_n_normal_count = []
        i_n_datetime_sorted = []
        for i in intrusion_datetime_count_raw:
            intrusion_container.update({bytes.decode(i[0]): int(i[1])})
        for i in normal_datetime_count_raw:
            normal_container.update({bytes.decode(i[0]): int(i[1])})
        i_n_datetime_sorted_ts = sorted(intrusion_container)  # Or normal_container, they're all the same.
        for i in i_n_datetime_sorted_ts:
            i_n_intrusion_count.append(intrusion_container[i])
        for i in i_n_datetime_sorted_ts:
            i_n_normal_count.append(normal_container[i])
        for i in i_n_datetime_sorted_ts:
            i_n_datetime_sorted.append(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(i))))

        # Real-time Tracking - redis queue receiving
        intrusion_pubsub = redis_connection.pubsub()
        intrusion_pubsub.subscribe("intrusion")
        intrusion_pubsub.parse_response()
        intrusion_set = eval(bytes.decode(intrusion_pubsub.parse_response()[2]))

        normal_pubsub = redis_connection.pubsub()
        normal_pubsub.subscribe("normal")
        normal_pubsub.parse_response()
        normal_set = eval(bytes.decode(normal_pubsub.parse_response()[2]))

        # Real-time Tracking - Map scatter info
        intrusion_scatter = []
        if type(intrusion_set) is not list:
            intrusion_set = [str(intrusion_set)]
        for item in intrusion_set:
            if type(item) is not dict:
                item = eval(item)
            req = requests.get("http://api.map.baidu.com/location/ip",
                               {'ak': '0jKbOcwqK7dGZiYIhSai5rsxTnQZ4UQt',
                                'ip': item['host'],
                                'coor': 'gcj02'})
            coord_x = eval(req.text)['content']['point']['x']
            coord_y = eval(req.text)['content']['point']['y']
            intrusion_scatter.append({"name": item['host'], "value": [coord_x,
                                                                      coord_y,
                                                                      item['data'],
                                                                      item['datetime']
                                                                      ]})
        # Redis persistence
        for item in intrusion_scatter:
            redis_connection.lpush("intrusion-scatter-all", item)

        normal_scatter = []
        if type(normal_set) is not list:
            normal_set = [str(normal_set)]
        for item in normal_set:
            if type(item) is not dict:
                item = eval(item)
            req = requests.get("http://api.map.baidu.com/location/ip",
                               {'ak': '0jKbOcwqK7dGZiYIhSai5rsxTnQZ4UQt',
                                'ip': item['host'],
                                'coor': 'gcj02'})
            coord_x = eval(req.text)['content']['point']['x']
            coord_y = eval(req.text)['content']['point']['y']
            normal_scatter.append({"name": item['host'], "value": [coord_x,
                                                                   coord_y,
                                                                   item['data'],
                                                                   item['datetime']
                                                                   ]})
        # Redis persistence
        for item in normal_scatter:
            redis_connection.lpush("normal-scatter-all", item)

        # History Tracking - Map scatter info
        # Bytes to json
        raw_intrusion_scatter_all_set = redis_connection.lrange("intrusion-scatter-all", 0, 1000)
        raw_normal_scatter_all_set = redis_connection.lrange("normal-scatter-all", 0, 1000)
        intrusion_scatter_all_set = []
        normal_scatter_all_set = []
        for e in raw_normal_scatter_all_set:
            decoded = eval(bytes.decode(e))
            normal_scatter_all_set.append(decoded)
        for e in raw_intrusion_scatter_all_set:
            decoded = eval(bytes.decode(e))
            intrusion_scatter_all_set.append(decoded)

        # Host Ranking - bytes to json
        host_ranking_set = []
        host = []
        host_visit = []
        for i in host_ranking:
            host.append(bytes.decode(i[0]))
            host_visit.append(int(i[1]))
        host_ranking_set.append({"hosts": host, "counts": host_visit})

        # Req Ranking - bytes to json
        req_ranking_set = []
        req = []
        req_count = []
        for i in req_ranking:
            req.append(bytes.decode(i[0]))
            req_count.append(int(i[1]))
        req_ranking_set.append({"reqs": req, "counts": req_count})

        # Pie board
        # Status code pie
        statuscode_set = []
        statuscode_legend = []
        for i in statuscode_raw:
            statuscode_set.append({"value": int(i[1]), "name": bytes.decode(i[0])})
            statuscode_legend.append(bytes.decode(i[0]))
        # Req method pie
        req_method_set = []
        req_method_legend = []
        for i in req_method_raw:
            req_method_set.append({"value": int(i[1]), "name": bytes.decode(i[0])})
            req_method_legend.append(bytes.decode(i[0]))

        # Req timeline board
        container = {}
        datetime_count = []
        for i in datetime_raw:
            container.update({bytes.decode(i[0]): int(i[1])})
        datetime_sorted = sorted(container)
        datetime = []
        for i in datetime_sorted:
            datetime_count.append(container[i])
            datetime.append(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(i))))

        # Console output
        print(total_req, '\n',
              visitor, '\n',
              request_type, '\n',
              traffic, '\n',
              log_size,'\n',
              intrusion_count, '\n',
              normal_count, '\n',
              intrusion_set, '\n',
              normal_set, '\n',
              intrusion_scatter, '\n',
              normal_scatter, '\n',
              intrusion_scatter_all_set, '\n',
              normal_scatter_all_set, '\n',
              host_ranking_set, '\n',
              req_ranking_set, '\n',
              statuscode_set, '\n',
              statuscode_legend, '\n',
              req_method_set, '\n',
              req_method_legend, '\n',
              datetime, '\n',
              datetime_count, '\n',
              datetime_raw, '\n',
              i_n_datetime_sorted, '\n',
              i_n_intrusion_count, '\n',
              i_n_normal_count)

        # SocketIO emitting
        socketio.emit('main',
                      {'count': count,
                       'total_req': int(total_req),
                       'valid_req': int(request_type[0]),
                       'not_found_req': int(request_type[1]),
                       'visitor': int(visitor),
                       'traffic': int(traffic/1024/1024),
                       'log_size': int(log_size/1024/1024),
                       'intrusion_count': int(intrusion_count),
                       'normal_count': int(normal_count),
                       'intrusion_scatter': intrusion_scatter,
                       'normal_scatter': normal_scatter,
                       'intrusion_scatter_all': intrusion_scatter_all_set,
                       'normal_scatter_all': normal_scatter_all_set,
                       'host_ranking_set': host_ranking_set,
                       'req_ranking_set': req_ranking_set,
                       'statuscode_set': statuscode_set,
                       'statuscode_legend': statuscode_legend,
                       'req_method_set': req_method_set,
                       'req_method_legend': req_method_legend,
                       'datetime': datetime,
                       'datetime_count': datetime_count,
                       'i_n_datetime_sorted': i_n_datetime_sorted,
                       'i_n_intrusion_count': i_n_intrusion_count,
                       'i_n_normal_count': i_n_normal_count}, namespace='/main')


@app.route('/')
def index():
    return render_template('dashboard.html', async_mode=socketio.async_mode)


@app.route('/about')
def about():
    return render_template('about.html', async_mode=socketio.async_mode)


@socketio.on('connect', namespace='/main')
def test_connect():
    global thread
    with thread_lock:
        if thread is None:
            socketio.start_background_task(target=background_thread)


if __name__ == '__main__':
    socketio.run(app, debug=True)
