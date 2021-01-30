import random


def unique_time_index_tranform(data):
    ts = sorted(list(set([d['time'] for d in data])))
    data_subs = []
    for t in ts:
        data_sub = []
        for d in data:
            if t == d['time']:
                data_sub.append(d)
            else:
                pass
        data_subs.append(data_sub)

    data_clean = []
    for ds in data_subs:
        ds_new = []
        for idx,d in enumerate(ds):
            temp = d
            t = d['time'] + random.random()
            d.update({'time':t})
            ds_new.append(d)
        data_clean += ds_new
    return data_clean