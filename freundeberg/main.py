'''
Created on 23/02/2016

@author: jorge
'''
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
import numpy as np
import scipy.sparse as sps
from freundeberg.utils.DateHelper import DateHelper

def get_parsed_line(line):
    startyear = 2000
    parts = line.split(';')
    year2 = parts[0].split('.')[2]
    year = 0
    if len(year2) == 4: 
        year = year2 
    else: 
        year=startyear+int(year2)
    month = float(parts[0].split('.')[1])
    day = float(parts[0].split('.')[0])
    hour = float(parts[1].split(':')[0])
    minutes = float(parts[1].split(':')[1])
    usage = float(parts[2].replace(",","."))
    return (usage,year,month,day,hour,minutes)


def start():
    conf = SparkConf().setAppName("test").setMaster("local")
    sc = SparkContext(conf=conf)
    rawData_lg= sc.textFile("/Users/jorge/Documents/Clients/freudenberg/Spark-Elec/data/LG_2015.csv")
    def filter_days(line):
        parts = line.split(";")
        if parts[0].find(".08.15")!=-1:
            return True
        else:
            return False
        
    only_2mon = rawData_lg.filter(filter_days)
    
    def get_tuples(line):
        tuples = get_parsed_line(line)
        return (str(tuples[1])+";"+str(tuples[2])+";"+str(tuples[3]), tuples[0])
    usagedData = rawData_lg.map(get_tuples)
    
    reduce_day = usagedData.reduceByKey(lambda a,b: a+b)
    counts_per_day = usagedData.countByKey()
    
    days_groupped = usagedData.groupByKey()
    top3_per_day = days_groupped.map(lambda line:  (line[0],sorted(line[1],reverse=True)[:2]))
    
    mapped_mean_days = sc.broadcast(top3_per_day.collectAsMap())
    sum_of_errors = sc.accumulator(0)
    number_of_days = 2
    def lag_seven_days_func(line):
        date = line[0].split(";")
        datehelper_from_value= DateHelper(date[0],date[1],date[2])
        day_usage_lagged = []
        a = ""
        for a  in datehelper_from_value.last_x_days(number_of_days):
                dates= a.split(";")
                datesfinal = str(float(dates[0]))+";"+str(float(dates[1]))+";"+str(float(dates[2]))
                try:
                    for z in mapped_mean_days.value.get(datesfinal):
                        day_usage_lagged += z
                except Exception as inst:  
                    print("Cannot find Date: ",datesfinal, inst)
                    sum_of_errors.add(1)
        #LabeledPoint(line._2, Vectors.dense(day_usage_lagged.toArray))
        return (line[1][0], day_usage_lagged
                                        .append(datehelper_from_value.is_tomorrow_off())
                                        .append(datehelper_from_value.was_yesterday_off()))    
    lag_seven_days = top3_per_day.map(lag_seven_days_func)
    lag_7days_clean = lag_seven_days.filter(lambda line: len(line[1])==(3*number_of_days)+2)
    labelpoint_lag7days = lag_7days_clean.map(lambda line: LabeledPoint(line[0],line[1]))
    (trainingData, testData) = labelpoint_lag7days.randomSplit((0.8, 0.2), 12344411L)
    trainingData.cache()
    testData.cache()
    impurity = "variance"
    maxDepth = 30
    maxBins = 300
    model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo={}, impurity,maxDepth, maxBins)
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(testData.count())
    print('Test Mean Squared Error = ' + str(testMSE))
    print('Learned regression tree model:')
    #print(model.toDebugString())
if __name__ == '__main__':
    start()