
import pandas as pd

exper = pd.read_csv('experiments_anonymised.txt')
print(exper)

max_cols = 20
data_thu = pd.read_csv('wp5Data1ThursBB3.csv',header=None,names=range(max_cols))
data_thu.columns = ['camera_id','dt','bt']+range(3,max_cols)
data_fri = pd.read_csv('wp5Data1FriBB3.csv',header=None,names=range(max_cols))
data_fri.columns = ['camera_id','dt','bt']+range(3,max_cols)
#data_thu = data_thu.dropna(axis=1,how='all')
#d = data_thu[:,2:max_cols]


# 3,1467900022735,11765191481,56794274,56794275

# respective camera id: b8aeed78cffe
# 1467900022735 is 1467900022.735 which is dt = 2016-07-07 15:00:22.735Z
# 11765191481 is bt = 11765191481
# 56794274,56794275 are bounding box ids

coords_thu = pd.read_csv('wp5bbCoordsFriBB3.csv',header=None)
coords_fri = pd.read_csv('wp5bbCoordsFriBB3.csv',header=None)
coords_thu.columns = ['bb_id','person_id','bb2d_0','bb2d_1','bb2d_2','bb2d_3','bb3d_0','bb3d_1','bb3d_2','bb3d_3','bb3d_4','bb3d_5']
coords_fri.columns = ['bb_id','person_id','bb2d_0','bb2d_1','bb2d_2','bb2d_3','bb3d_0','bb3d_1','bb3d_2','bb3d_3','bb3d_4','bb3d_5']

# 56794274, -1, 76, 8, 347, 403, -1107, 1053, 2588, 150, -911, 3188
# 56794275, 1, 300, 56, 383, 371, -86, 796, 2469, 339, -704, 3069
bb2person = {}
for i in range(10000): #range(coords_thu.shape[0]):
    bb2person[coords_thu.bb_id[i]] = coords_thu.person_id[i]

#person_occurrences = {timestamp:location}
# create a dataframe with dt,camera_id,person_id
#
# add to it the experiment_id and wearable_id

a=1


