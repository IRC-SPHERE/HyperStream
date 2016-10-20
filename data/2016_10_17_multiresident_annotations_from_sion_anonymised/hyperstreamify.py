
import pandas as pd

exper = pd.read_csv('occurrence_times_manually.txt')
print(exper)

max_cols = 20
data_thu = pd.read_csv('wp5Data1ThursBB3.csv',header=None,names=range(max_cols))
data_thu.columns = ['camera_id','dt','bt']+range(3,max_cols)
data_fri = pd.read_csv('wp5Data1FriBB3.csv',header=None,names=range(max_cols))
data_fri.columns = ['camera_id','dt','bt']+range(3,max_cols)
data = data_thu.append(data_fri)
data.index = range(data.shape[0])

# 3,1467900022735,11765191481,56794274,56794275

# respective camera id: b8aeed78cffe
# 1467900022735 is 1467900022.735 which is dt = 2016-07-07 15:00:22.735Z
# 11765191481 is bt = 11765191481
# 56794274,56794275 are bounding box ids

coords_thu = pd.read_csv('wp5bbCoordsThursBB3.csv',header=None)
coords_fri = pd.read_csv('wp5bbCoordsFriBB3.csv',header=None)
coords_thu.columns = ['bb_id','person_id','bb2d_0','bb2d_1','bb2d_2','bb2d_3','bb3d_0','bb3d_1','bb3d_2','bb3d_3','bb3d_4','bb3d_5']
coords_fri.columns = ['bb_id','person_id','bb2d_0','bb2d_1','bb2d_2','bb2d_3','bb3d_0','bb3d_1','bb3d_2','bb3d_3','bb3d_4','bb3d_5']
coords = coords_thu.append(coords_fri)
coords.index = range(coords.shape[0])

# 56794274, -1, 76, 8, 347, 403, -1107, 1053, 2588, 150, -911, 3188
# 56794275, 1, 300, 56, 383, 371, -86, 796, 2469, 339, -704, 3069
bb2person = {}
print('working through coords')
for (i,row) in coords.iterrows():
    if (i % 10000)==0:
        print(i)
    if row.person_id>=1:
        bb2person[row.bb_id] = row.person_id

res = pd.DataFrame()
rows = []
out_of_experiment = {}
print('working through data')
for i in range(data.shape[0]):
    if (i % 10000)==0:
        print(i)
    bb_ids = data.loc[i][3:max_cols]
    for bb_id in bb_ids:
        if bb2person.has_key(bb_id) and bb2person[bb_id]>=1:
            exp_id = -1
            wearable_id = 'NA'
            for j in range(exper.shape[0]):
                if (exper.person_id[j]==bb2person[bb_id]) and (exper.first_occurrence[j]<=data.dt[i]) and (exper.last_occurrence[j]>=data.dt[i]):
                    exp_id = exper.experiment[j]
                    wearable_id = exper.wearable[j]
            if exp_id>0:
                rows.append({'dt':data.dt[i],'camera_id':data.camera_id[i],'person_id':bb2person[bb_id],'exper_id':exp_id,'wearable_id':wearable_id})
            else:
                out_of_experiment[bb2person[bb_id]] = data.dt[i] # on Friday video was switched on some time between experiments 6 and 7 and the persons 5, 13, 25 got annotated in that range - we discard that information here
res = pd.DataFrame(rows)
print(out_of_experiment)
# {25: 1467985373054, 5: 1467985361449, 13: 1467985377428}

res.to_csv('location_annotations.csv', sep=',', index=False)


# in one experiment person 1 forgot to put on the wearable


#w = res.loc[res['person_id'] == 24]
#w = res.loc[(res['person_id'] == 9)] # and (res['dt']>0)]
#print(w.iloc[-1])

#print(res.loc[res['person_id']==9].iloc[0])

#w = res.loc[(res['person_id'] == 5)] # and (res['dt']>0)]
#print(w.iloc[-1])


