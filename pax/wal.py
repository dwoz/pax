import io
import collections.abc
import asyncio
import aiofiles


NOVAL = object()

class Wal(object):

    def __init__(self, name, _fd=None):
        self.name = name
        self.wid = 0
        self._fd = _fd

    async def open(self, path_name):
        if self._fd is None:
            self._fd = await aiofiles.open(path_name, 'ab+')

    async def add(self, op, key, value=NOVAL):
        wid = self.wid
        self.wid += 1
        record = WalRecord(wid, op, key, value)
        await self.write(str(record).encode('utf-8'))

    async def seek(self, index, whence=0):
        await self._fd.seek(index, whence)

    async def write(self, line):
        await self._fd.write(line)

    async def read(self, length=None):
        return await self._fd.read(length)

    async def index(self):
        return await self._fd.tell()

    async def close(self):
        await self._fd.close()

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, *args):
        await self._fd.close()

#    def __enter__(self):
#        await self.open()
#        return self
#
#    def __exit__(self, *args):
#        self._fd.close()
#
#    def __iter__(self):
#        self.seek(0)
#        for line in self._fd:
#            yield WalRecord.parse(line)

    def __aiter__(self):
        return self

    async def __anext__(self):
        line = await self._fd.readline()
        if line:
            return WalRecord.parse(line)
        raise StopAsyncIteration



class WalRecord(object):

    def __init__(self, wid, op, key, value=NOVAL):
        self.wid = wid
        self.op = op
        self.key = key
        self.value = value

    def __str__(self):
        if self.value is NOVAL:
            return "{}\t{}\t{}\t\n".format(self.wid, self.op, self.key)
        return "{}\t{}\t{}\t{}\n".format(self.wid, self.op, self.key, self.value)

    @classmethod
    def parse(cls, line):
        # Remove trailing newline and decode
        line = line[:-1].decode('utf-8')
        swid, op, key, val = line.split('\t')
        if op == 'DEL':
            val = NOVAL
        return cls(int(swid), op, key, val)


#class Table(collections.abc.MutableMapping):
class Table(object):

    def __init__(self, name, path_name=None, _wal=None):
        self.name = name
        self.path_name = path_name
        if self.path_name is None:
            self.path_name = '{}.wal.0'.format(self.name)
        self._data = {}
        self.wal = _wal

    @property
    def wid(self):
        return self.wal.wid

    async def open(self):
        self.wal = Wal(self.path_name)
        await self.wal.open(self.path_name)
        await self._sync()

    async def _sync(self):
        await self.wal.seek(0)
        async for record in self.wal:
            if record.op == 'SET':
                self._data.__setitem__(record.key, record.value)
            elif record.op == 'DEL':
                self._data.__delitem__(record.key)

    def get(self, key):
        return self._data.__getitem__(key)

    #def __getitem__(self, key):
    #    return self._data.__getitem__(key)

    async def set(self, key, value):
        await self.wal.add("SET", key, value)
        self._data.__setitem__(key, value)

    #def __setitem__(self, key, value):
    #    self.wal.add("SET", key, value)
    #    self._data.__setitem__(key, value)

    async def delitem(self, key):
        await self.wal.add("DEL", key)
        self._data.__delitem__(key)

    #def __delitem__(self, key):
    #    self.wal.add("DEL", key)
    #    self._data.__delitem__(key)

    def __iter__(self):
        return self._data.__iter__()

    def __aiter__(self):
        return self._data

    def __len__(self):
        return self._data.__len__()

    async def __aenter__(self):
        await self.open()
        return self

    def __exit__(self, *args):
        self.wal.close()

    async def __aexit__(self, *args):
        await self.wal.close()


def xmain():
    with Table('foo') as tbl:
        tbl['a'] = 'b'
        tbl['b'] = 'b'
        tbl['c'] = 'c'
        tbl['a'] = 'foo'
        tbl.pop('c')
    with Table('foo') as tbl:
        print(tbl['a'])
        print(tbl['b'])
        print('c' in tbl)

async def main():
    async with Table('foo') as tbl:
        await tbl.set('a', 'b')
        await tbl.set('b', 'b')
        await tbl.set('c', 'c')
        await tbl.set('a', 'foo')
        await tbl.delitem('c')
    async with Table('foo') as tbl:
        await tbl._sync()
        print(tbl.get('a'))
        print(tbl.get('b'))
        print('c' in tbl)

if __name__ == "__main__":
    asyncio.run(main())
