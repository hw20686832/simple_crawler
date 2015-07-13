# coding:utf-8


def group(it, n, fill=False):
    q = 0
    for i in xrange(0, len(it), n):
        sli = it[i:(q+1)*n]
        for x in xrange(n-len(sli)):
            if fill is not False:
                sli.append(fill)

        yield sli
        q += 1


def class_import(name):
    module, pclass = name.rsplit('.', 1)
    mod = __import__(module)

    cls = getattr(mod, pclass)
    return cls


if __name__ == '__main__':
    l = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    print list(group(l, 2, fill=None))
