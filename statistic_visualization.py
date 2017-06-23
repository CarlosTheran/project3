from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import sys
import numpy as np
def sparksessioninit(sparkConf):
    if ('sparksession' not in globals()):
       globals()['sparksession']=SparkSession.builder.config(conf=sparkConf). \
       enableHiveSupport().getOrCreate()
    return globals()['sparksession']

def ML_visual():
   
   #tiempo, fecha = sys.argv[1:]
   #interval_time =("%s %s"%(fecha,tiempo))
   query_MAGA= "select prediction,sum(total_label) as total_global from sentimental_analysis where label = 'MAGA' \
            group by prediction order by total_global"

   query_DICTATOR= "select prediction,sum(total_label) as total_global from sentimental_analysis where label = 'DICTATOR' \
            group by prediction order by total_global"

   query_IMPEACH= "select prediction,sum(total_label) as total_global from sentimental_analysis where label = 'IMPEACH' \
            group by prediction order by total_global"

   query_DRAIN= "select prediction,sum(total_label) as total_global from sentimental_analysis where label = 'DRAIN' \
            group by prediction order by total_global"

   query_SWAMP= "select prediction,sum(total_label) as total_global from sentimental_analysis where label = 'SWAMP' \
            group by prediction order by total_global"

   query_COMEY= "select prediction,sum(total_label) as total_global from sentimental_analysis where label = 'COMEY' \
            group by prediction order by total_global"
   fig1,plt1 =plt.subplots()
   ploting(query_MAGA,'MAGA_Statistic',plt1,fig1)

   fig2,plt2 =plt.subplots()
   ploting(query_DICTATOR,'DICTATOR_Statistic',plt2,fig2)

   fig3,plt3 =plt.subplots()
   ploting(query_DRAIN,'DRAIN_Statistic',plt3,fig3)

   fig4,plt4 =plt.subplots()
   ploting(query_SWAMP,'SWAMP_Statistic',plt4,fig4)

   fig5,plt5 =plt.subplots()
   ploting(query_COMEY,'COMEY_Statistic',plt5,fig5)
   




def ploting(query_MAGA,string,plt,fig):

    spark = sparksessioninit(sc.getConf())
    data = spark.sql(query_MAGA)
    data.show() 
    df = data.toPandas()


    labels = 'negative', 'positive'
    colors = ['gold','lightcoral']
    explode = (0, 0)  # explode 1st slice
 
# Plot

    plt.pie(df['total_global'], explode=explode, labels=labels, colors=colors, autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')
    plt.set_title(string, bbox={'facecolor':'0.8', 'pad':5})
    fileName='/home/carlos_theran/project3/'+string+'.pdf' 
    fig.savefig(fileName, bbox_inches='tight')
   


if __name__ == "__main__":
    sc = SparkContext(appName="Statistic_Visualization")
    ML_visual()
