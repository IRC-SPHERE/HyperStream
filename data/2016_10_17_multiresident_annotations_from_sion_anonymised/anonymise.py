
import pandas as pd

exper = pd.read_csv('experiments.txt',header=None)
exper.columns = ['day','experiment','wearable','name']
print(exper)


id2name = pd.read_csv('people_id2name.txt',header=None)
id2name.columns = ['idx','name']
id2name['id'] = id2name.idx.str.slice(2,4).convert_objects(convert_numeric=True)
print(id2name)

name2id = {}
for i in range(id2name.shape[0]):
    name2id[id2name.name[i]] = id2name.id[i]
print(name2id)

exper['person_id'] = [name2id[name] for name in exper['name']]

del exper['name']

exper.to_csv('experiments_anonymised.txt', sep=',', index=False)

print(exper)

