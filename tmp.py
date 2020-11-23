import socket
import os
import fcntl
sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
sock.setblocking(0)
fcntl.fcntl(sock, fcntl.F_SETFL, os.O_NONBLOCK)
sock.settimeout(0)
sock.bind(('127.0.0.1', 12324))
sock.listen(5)
while True:
    try:
        conn, addr = sock.accept()
    except BlockingIOError:
        pass
    else:
        break
print("Got connection {!r} {!r}".format(conn, addr))

a = None
while True:
    try:
        a = conn.recv(1024, 0x40)
    except BlockingIOError:
        pass
    else:
        break

print(repr(a))
