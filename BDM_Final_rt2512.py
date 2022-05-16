import csv
import json
import sys
import pyspark
from pyspark.sql import SparkSession

from pyproj import Transformer
from shapely.geometry import Point

if __name__ == "__main__":
    sc = pyspark.SparkContext.getOrCreate()
    spark = SparkSession(sc)

    
    rdd_df = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020').map(lambda x: next(csv.reader([x])))
    title = rdd_df.first()
    rdd_df = rdd_df.filter(lambda row : row != title) 
    filter = sc.textFile('nyc_supermarkets.csv')
    rdd_df1 = rdd_df.map(lambda x: [x[0], '-'.join(x[12].split('T')[0].split('-')[:2]), '-'.join(x[13].split('T')[0].split('-')[:2]), x[18], json.loads(x[19])])

    list_of_filters = filter.map(lambda x: x.split(',')[-2]).collect()
    rdd_df1 = rdd_df1.filter(lambda x: x[0] in list_of_filters)

    def list_of_dates(date_1,date_2,home_cbgs):
        if date_1 =='2019-03' or date_2 == '2019-03':
            return [home_cbgs,{},{},{}]
        elif date_1 =='2019-10' or date_2 == '2019-10':
            return [{},home_cbgs,{},{}]
        elif date_1 =='2020-03' or date_2 == '2020-03':
            return [{},{},home_cbgs,{}]
        elif date_1 =='2020-10' or date_2 == '2020-10':
            return [{},{},{},home_cbgs]
        else:
            None

    def lists_of_key(x,y):
        result = [{},{},{},{}]
        for i in range(len(x)):
            result[i].update(x[i])
            result[i].update(y[i])
        return result

    rdd_df2 = rdd_df1.map( lambda x: (x[3],list_of_dates(x[1],x[2],x[4]))).filter(lambda x: x[1] is not None).reduceByKey(lambda x,y: lists_of_key(x,y))

    cbg_centroids = sc.textFile('nyc_cbg_centroids.csv')
    title2 = cbg_centroids.first()
    cbg_centroids = cbg_centroids.filter(lambda row : row != title2) 
    filter_of_cbg = cbg_centroids.map(lambda x: x.split(',')[0]).collect()

 
    def cbg_filtering(dictionary,list_filters):
        result = []
        for dict_ in dictionary:
            if dict_ == {}: result.append('')
            else:
                dict_output = []
                for item in dict_:
                    if item in list_filters:
                        dict_output.append((item,dict_[item]))
                if dict_output != []:  
                    result.append(dict_output)
                else:
                    result.append('')
        return result

    rdd_df3 = rdd_df2.map(lambda x: [x[0],cbg_filtering(x[1],filter_of_cbg)])

    rdd_list_cbg = cbg_centroids.map(lambda x: [x.split(',')[0],x.split(',')[1],x.split(',')[2]]).collect()

    def transformcbg(count,transfer_list):
        t = Transformer.from_crs(4326, 2263)
        if type(count) == list: 
            list_output = []
            for dict_ in count:
                if dict_ == '': list_output.append('')
                else:
                    dict_output = []
                    for item1 in dict_:
                        for item2 in transfer_list:
                            if item1[0] == item2[0]:
                                dict_output.append((t.transform(item2[1],item2[2]),item1[1]))
                    list_output.append(dict_output)
            return list_output
        else:
            for item in transfer_list:
                if count == item[0]:
                    return t.transform(item[1],item[2])

    rdd_df4 = rdd_df3.map(lambda x: [x[0],transformcbg(x[0],rdd_list_cbg),transformcbg(x[1],rdd_list_cbg)])
    def distance(start_list,destination):
        result = []
        for item in start_list:
            if item == '':
                result.append('')
            else:
                distance_list=[]
                for start in item:
                    distance_list.append((Point(start[0][0],start[0][1]).distance(Point(destination[0],destination[1]))/5280,start[1]))
                result.append(distance_list)
        return result

    rdd_df4 = rdd_df4.map(lambda x: [x[0],distance(x[2],x[1])])


    def avg_dist(input):
        output = []
        for item in input:
            if item == '':
                output.append('')
            else:
                sum_ = 0
                num_ = 0
                for cuple in item:
                    sum_ += cuple[0] * cuple[1]
                    num_ += cuple[1]
                if num_ != 0:
                    output.append(str(round(sum_/num_,2)))
        return output
    rdd_df5 = rdd_df4.map(lambda x: [x[0],avg_dist(x[1])])

    output1 = rdd_df5.map(lambda x: [str(x[0]),str(x[1][0]),str(x[1][1]) ,str(x[1][2]),str(x[1][3])])\
            .toDF(['cbg_fips', '2019-03' , '2019-10' , '2020-03' , '2020-10'])\
            .sort('cbg_fips', ascending = True)

    output1.coalesce(1).write.options(header='true').csv(sys.argv[1])