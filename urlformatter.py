def urlformat(x) :
    if x.endswith('/'):
        x = x[:-1]
    if x.endswith('htm') or x.endswith('html'):
        pos = x.rfind('/')
        x = x[:pos]
    return x