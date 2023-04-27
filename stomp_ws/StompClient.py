import json
import logging
import time
from threading import Thread

import websocket
from apscheduler.schedulers.background import BackgroundScheduler

from .Frame import Frame

VERSIONS = '1.0,1.1'

SOCKET_HEARTBEAT_JOB_ID = 'SocketHeartbeatJobId'
SOCKET_RECONNECT_JOG_ID = "SocketReconnectJobId"
DEFAULT_CONNECT_TIMEOUT_SECOND = 5


class StompClient:

    def __init__(self, url, heartbeat_interval=10):

        self.url = url
        self.ws = websocket.WebSocketApp(self.url)
        self.ws.on_open = self._on_open
        self.ws.on_message = self._on_message
        self.ws.on_error = self._on_error
        self.ws.on_close = self._on_close

        self.opened = False

        self.connected = False
        self.heartbeat_interval = heartbeat_interval
        self.subscribe_counter = 0
        self.reconnect_counter = 0
        self.subscriptions = {}

        self._connectCallback = None
        self.errorCallback = None
        self._init_scheduler()
        # 连接头
        self.connect_header = None
        # 订阅头
        self.subscribe_header = {}
        self.reconnecting = False
        self.connect_timeout = DEFAULT_CONNECT_TIMEOUT_SECOND

    def _init_scheduler(self):
        self._scheduler = BackgroundScheduler()
        self._scheduler.start()

    def _connect(self, timeout=0):
        thread = Thread(target=self.ws.run_forever)
        thread.daemon = False
        thread.start()

        total_second = 0
        while self.opened is False:
            time.sleep(.25)
            total_second += 0.25
            if 0 < timeout < total_second:
                raise TimeoutError(f"Connection to {self.url} timed out")

    def _on_open(self, ws_app, *args):
        logging.info('Socket On Open >>>>>>>>>>>>>>>>>>>>>>>>>')
        self.opened = True
        self.reconnect_counter = 0
        # 心跳
        self._scheduler.add_job(self._heartbeat, 'interval', seconds=self.heartbeat_interval,
                                id=SOCKET_HEARTBEAT_JOB_ID)
        # 移除重连任务
        if self._scheduler.get_job(SOCKET_RECONNECT_JOG_ID) is not None:
            self._scheduler.remove_job(SOCKET_RECONNECT_JOG_ID)
        if self.reconnecting:
            # 重新订阅
            self._resubscribe()
        self.reconnecting = False

    def _on_close(self, ws_app, *args):
        logging.warning("Whoops! Lost connection to " + self.ws.url)
        self._clean_up()
        self.reconnect()

    def _on_error(self, ws_app, error, *args):
        logging.error(f'On Error: {self.url}')
        logging.exception(error)

    def _on_message(self, ws_app, message, *args):
        logging.debug("\n<<< " + str(message))
        if message.startswith('a'):
            message = message[1:]
        if message.startswith('[') and message.endswith(']'):
            messages = json.loads(message)
            message = messages[0]
        frame = Frame.unmarshall_single(message)
        _results = []
        if frame.command == "CONNECTED":
            self.connected = True
            logging.debug("connected to server " + self.url)
            if self._connectCallback is not None:
                _results.append(self._connectCallback(frame))
        elif frame.command == "MESSAGE":

            subscription = frame.headers['subscription']
            logging.info(subscription)
            if subscription in self.subscriptions:
                on_receive = self.subscriptions[subscription]
                if on_receive is None:
                    return _results
                message_id = frame.headers['message-id']

                def ack(headers):
                    if headers is None:
                        headers = {}
                    return self.ack(message_id, subscription, headers)

                def nack(headers):
                    if headers is None:
                        headers = {}
                    return self.nack(message_id, subscription, headers)

                frame.ack = ack
                frame.nack = nack

                _results.append(on_receive(frame))
            else:
                info = "Unhandled received MESSAGE: " + str(frame)
                logging.debug(info)
                _results.append(info)
        elif frame.command == 'RECEIPT':
            pass
        elif frame.command == 'ERROR':
            if self.errorCallback is not None:
                _results.append(self.errorCallback(frame))
        else:
            info = "Unhandled received MESSAGE: " + frame.command
            logging.debug(info)
            _results.append(info)

        return _results

    def _transmit(self, command, headers, body=None):
        if not self.opened:
            logging.warning(f'与服务器断开连接: {self.ws.url}')
            return
        payload = []
        out = Frame.marshall(command, headers, body)
        logging.debug("\n>>> " + out)
        payload.append(out)
        self.ws.send(json.dumps(payload))

    def connect(self, username=None, password=None, headers=None, connect_callback=None, error_callback=None,
                timeout=5):
        if timeout <= 0:
            timeout = DEFAULT_CONNECT_TIMEOUT_SECOND
        else:
            self.connect_timeout = timeout
        logging.debug("Opening web socket...")
        self._connect(timeout)

        headers = headers if headers is not None else {}
        headers['host'] = self.url
        headers['accept-version'] = VERSIONS
        headers['heart-beat'] = f'{self.heartbeat_interval},{self.heartbeat_interval}'

        if username is not None:
            headers['username'] = username
        if password is not None:
            headers['password'] = password

        self._connectCallback = connect_callback
        self.errorCallback = error_callback
        self.connect_header = headers
        self._transmit('CONNECT', headers)

    # 断线重连
    def reconnect(self):
        self.reconnecting = True
        # 尝试重连
        if self._scheduler.get_job(SOCKET_RECONNECT_JOG_ID) is not None:
            logging.info('重连任务已存在')
            return
        self._scheduler.add_job(self._reconnect, 'interval', seconds=self.connect_timeout + 1, id=SOCKET_RECONNECT_JOG_ID)

    def _reconnect(self):
        logging.info(f'尝试第{self.reconnect_counter}次重新建立链接')
        if self.opened:
            logging.info(f'尝试{self.reconnect_counter}次后, 重新建立socket连接')
            logging.info(f'尝试{self.reconnect_counter}次后, 重新建立socket连接')
            return
        self.reconnect_counter += 1
        self.connect(headers=self.connect_header, timeout=5)

    def disconnect(self, disconnect_callback=None, headers=None):
        if headers is None:
            headers = {}

        self._transmit("DISCONNECT", headers)
        self.ws.on_close = None
        self.ws.close()
        self._clean_up(disconnect_callback)

    def _resubscribe(self):
        # 重新订阅
        for headers in self.subscribe_header.values():
            self.subscribe(headers['destination'], headers=headers, callback=self.subscriptions[headers['id']])

    def _clean_up(self, disconnect_callback=None):
        self.opened = False
        self.connected = False
        # 移除心跳任务
        if self._scheduler.get_job(SOCKET_HEARTBEAT_JOB_ID) is not None:
            logging.info('移除心跳任务')
            self._scheduler.remove_job(SOCKET_HEARTBEAT_JOB_ID)
        if self._scheduler.get_job(SOCKET_RECONNECT_JOG_ID) is not None:
            logging.info('移除重连任务')
            self._scheduler.remove_job(SOCKET_RECONNECT_JOG_ID)
        if disconnect_callback is not None:
            disconnect_callback()

    def send(self, destination, headers=None, body=None):
        if headers is None:
            headers = {}
        if body is None:
            body = ''
        headers['destination'] = destination
        return self._transmit("SEND", headers, body)

    def subscribe(self, destination, callback=None, headers=None, reconnect=False):
        if headers is None:
            headers = {}
        if 'id' not in headers:
            headers["id"] = "sub-" + str(self.subscribe_counter)
            self.subscribe_counter += 1
        headers['destination'] = destination
        self.subscriptions[headers["id"]] = callback
        if not reconnect:
            # 用于重连重新订阅
            self.subscribe_header[headers["id"]] = headers
        self._transmit("SUBSCRIBE", headers)

        def unsubscribe():
            self.unsubscribe(headers["id"])

        return headers["id"], unsubscribe

    def _heartbeat(self):
        payload = ['\n']
        self.ws.send(json.dumps(payload))
        logging.debug('Send Heartbeat')

    def unsubscribe(self, id):
        logging.debug(f'Unsubscribe {id}')
        del self.subscriptions[id]
        return self._transmit("UNSUBSCRIBE", {
            "id": id
        })

    def ack(self, message_id, subscription, headers):
        if headers is None:
            headers = {}
        headers["message-id"] = message_id
        headers['subscription'] = subscription
        return self._transmit("ACK", headers)

    def nack(self, message_id, subscription, headers):
        if headers is None:
            headers = {}
        headers["message-id"] = message_id
        headers['subscription'] = subscription
        return self._transmit("NACK", headers)
