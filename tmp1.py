import time
import socket
sock = socket.socket()
sock.connect(('127.0.0.1', 12324))
sock.send(b'a')
time.sleep(15)
