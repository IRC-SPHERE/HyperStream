from hyperstream import Tool, date, delta


class Pool(Tool):
    def __str__(self):
        return __name__

    def __hash__(self):
        return hash(__name__)

    def process_params(self, timer, data, func):
        print('Defining a Pool stream')
        return [], {'timer': timer, 'data': data, 'func': func}

    def __call__(self, stream_def, start, end, writer, timer, data, func):
        print('Pool running from ' + str(start) + ' to ' + str(end))
        rel_start = stream_def.kwargs['data'].start
        rel_end = stream_def.kwargs['data'].end
        window = []
        future = []
        for (t, _) in timer:
            while (len(window) > 0) and (window[0][0] <= t + rel_start):
                window = window[1:]
            while (len(future) > 0) and (future[0][0] <= t + rel_end):
                doc = future[0]
                future = future[1:]
                if t + rel_start < doc[0] <= t + rel_end:
                    window.append(doc)
            while True:
                try:
                    doc = next(data)
                    if t + rel_start < doc[0] <= t + rel_end:
                        window.append(doc)
                    elif doc[0] > t + rel_end:
                        future.append(doc)
                        break
                except StopIteration:
                    break
            # single-document case:
            writer([(t, func((doc for doc in window)))])
            # multi-document case:

# for x in func( (doc for doc in window) ):
#        writer([(t,x)])
