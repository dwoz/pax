import io
def parse_conf(name='conf'):
    conf = {}
    with io.open(name, 'r') as fp:
        lines = fp.readlines()
    for line in lines:
        line = line.strip()
        if line:
            name, ip, port = line.split(':')
            if name in conf:
                raise Exception("Multiple configs for name")
            conf[name] = (ip, int(port))
    return conf
