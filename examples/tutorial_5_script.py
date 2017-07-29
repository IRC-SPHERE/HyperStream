import sys
sys.path.append("../") # Add parent dir in the Path

from hyperstream import HyperStream, Workflow
from hyperstream import TimeInterval
from hyperstream.utils import UTC
import hyperstream

from datetime import datetime
from utils import plot_high_chart
from utils import plot_multiple_stock
from dateutil.parser import parse

hs = HyperStream(loglevel=30)
print(hs)
print([p.channel_id_prefix for p in hs.config.plugins])

def dateparser(dt):
    return parse(dt.replace('M', '-')).replace(tzinfo=UTC)

ti_all = TimeInterval(datetime(1999, 1, 1).replace(tzinfo=UTC),
                      datetime(2013, 1, 1).replace(tzinfo=UTC))
ti_sample = TimeInterval(datetime(2007, 1, 1).replace(tzinfo=UTC),
                         datetime(2007, 3, 1).replace(tzinfo=UTC))

# M will be the Memory Channel
M = hs.channel_manager.memory

countries_list = ['Asia', 'Australia', 'NZ', 'USA']


countries_dict = {
    'Asia': ['BangkokMin', 'BangkokMax', 'HongKongMax', 'HongKongMin', 'KualaLumpurMax', 'KualaLumpurMin',
             'NewDelhiMax', 'NewDelhiMin', 'TokyoMax', 'TokyoMin'],
    'Australia': ['BrisbaneMax', 'BrisbaneMin', 'Canberramax', 'CanberraMin', 'GoldCoastMax', 'GoldCoastMin',
                  'MelbourneMin', 'Melbournemax',  'SydneyMax', 'SydneyMin'],
    'NZ': ['AucklandMax', 'AucklandMin', 'ChristchurchMax', 'ChristchurchMin', 'DunedinMax', 'DunedinMin',
           'HamiltonMax', 'HamiltonMin','WellingtonMax', 'WellingtonMin'],
    'USA': ['ChicagoMin', 'ChicagoMax', 'HoustonMax', 'HoustonMin', 'LosAngelesMax', 'LosAngelesMin',
            'NYMax', 'NYMin', 'SeattleMax', 'SeattleMin']
}

# delete_plate requires the deletion to be first childs and then parents
for plate_id in ['C.C', 'C']:
    if plate_id in [plate[0] for plate in hs.plate_manager.plates.items()]:
        print('Deleting plate ' + plate_id)
        hs.plate_manager.delete_plate(plate_id=plate_id, delete_meta_data=True)

for country in countries_dict:
    id_country = 'country_' + country
    if not hs.plate_manager.meta_data_manager.contains(identifier=id_country):
        hs.plate_manager.meta_data_manager.insert(
            parent='root', data=country, tag='country', identifier=id_country)
    for city in countries_dict[country]:
        id_city = id_country + '.' + 'city_' + city
        if not hs.plate_manager.meta_data_manager.contains(identifier=id_city):
            hs.plate_manager.meta_data_manager.insert(
                parent=id_country, data=city, tag='city', identifier=id_city)

C = hs.plate_manager.create_plate(plate_id="C", description="Countries", values=[], complement=True,
                                  parent_plate=None, meta_data_id="country")
CC = hs.plate_manager.create_plate(plate_id="C.C", description="Cities", values=[], complement=True,
                                   parent_plate="C", meta_data_id="city")


print hs.plate_manager.meta_data_manager.global_plate_definitions

from hyperstream import Workflow

# parameters for the csv_mutli_reader tool
csv_params = dict(
    filename_template='data/TimeSeriesDatasets_130207/Temp{}.csv',
    datetime_parser=dateparser, skip_rows=0, header=True)

def mean(x):
    return float(sum(x)) / max(len(x), 1)

with Workflow(workflow_id='tutorial_05', name='tutorial_05', owner='tutorials',
              description='Tutorial 5 workflow', online=False) as w:

    country_node = w.create_node(stream_name='raw_data', channel=M, plates=[C])
    city_node = w.create_node(stream_name='temperature', channel=M, plates=[CC])
    # FIXME Are these correct?
    country_node_avg_temp = w.create_node(stream_name='avg_temp', channel=M, plates=[C])
    # FIXME Should I create a node outside the plates?
    world_node_avg_temp = w.create_node(stream_name='avg_temp', channel=M, plates=[])

    for c in C:
        country_node[c] = hs.plugins.data_importers.factors.csv_multi_reader(source=None, **csv_params)
        for cc in CC[c]:
            city_node[cc] = hs.factors.splitter_from_stream(source=country_node[c],
                                                            splitting_node=country_node[c],
                                                            use_mapping_keys_only=True)
        country_node_avg_temp[c] = hs.factors.aggregate(sources=[city_node],
                                                        alignment_node=None,
                                                        aggregation_meta_data='city',
                                                        func=mean)
    # FIXME Should I create a node for a stream outside the plates?
    world_node_avg_temp = hs.factors.aggregate(sources=[country_node_avg_temp],
                                               alignment_node=None,
                                               aggregation_meta_data='country',
                                               func=mean)
    w.execute(ti_all)

for stream_id, stream in country_node.streams.iteritems():
    print(stream_id)
    print(stream.window().first())

for stream_id, stream in city_node.streams.iteritems():
    print(stream_id)
    print(stream.window().first())

for stream_id, stream in country_node_avg_temp.streams.iteritems():
    print(stream_id)
    print(stream.window().first())

for key, value in world_node_avg_temp.iteritems():
    print("[{}]: {}".format(key, value))
