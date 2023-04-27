import time
import uuid

from stomp_ws.Frame import Frame
from stomp_ws.StompClient import StompClient

server_id = str(uuid.uuid1()).replace('-', '')
session_id = str(uuid.uuid1()).replace('-', '')
websocket_client = StompClient(url=f'ws://localhost:8082/ws/stomp/{server_id}/{session_id}/websocket')
connect_header = {
    'userId': '12323',
    'clientId': 'abcdefs'
}


def on_message(frame: Frame):
    # 消息类型
    print(f'command: {frame.command}')
    # 消息头
    print(f'headers: {frame.headers}')
    # 消息体
    print(f'body: {frame.body}')


# 建立连接
websocket_client.connect(headers=connect_header)
# 订阅topic, callback 收到消息后的回调函数
subscribe_id, unsubscribe_func = websocket_client.subscribe(destination='/user/demo/pong', callback=on_message)


time.sleep(10)
print('取消订阅')
# 取消订阅，方式1
unsubscribe_func()
# 取消订阅方式2
# websocket_client.unsubscribe(subscribe_id)
print('断开连接')
# 断开连接
websocket_client.disconnect()
