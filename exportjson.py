# !/usr/bin/python  
# -*- coding:utf-8 -*-  
  
import json,MySQLdb  
import config
from datetime import datetime


try:  
    conn = MySQLdb.connect(config.serverAddr,config.user,config.password)  
    cur = conn.cursor()
    conn.select_db(config.databaseName)
    print('connet to ' + config.serverAddr+' databases: '+config.databaseName)
except MySQLdb.Error,msg:  
    print "MySQL connet error %d: %s" %(msg.args[0],msg.args[1])
tableName = raw_input('please input the table name which will be parse to json... :')
    
  
def tableToJson(table):
    #获取表字段
    rowItem = [] #保存所有的表字段
    descsql = 'desc %s' %table
    cur.execute(descsql)
    row = cur.fetchall()
    for item in row:
        rowItem.append(item[0])
    print('row',rowItem)

    #获取表长度
    countsql = 'select count(*) from %s' %table
    cur.execute(countsql)
    # print('rowcount',cur.rowcount)
    tableLenght = cur.fetchone()[0]#获取表的长度
    print('table count :', tableLenght)

    BaseLoop = config.BaseLoop#循环输出的基准长度。可以用输入的方式
    start = 0 #查询表的起点位置
    loopCount = tableLenght/BaseLoop#循环总次数-1
    last = tableLenght%BaseLoop#最后一次的起点
    loopIndex = 0 #循环指针
    f = open(tableName+'.json','a+')
    while(loopIndex <= loopCount):
        start = BaseLoop*loopIndex #重新设置起点
        if(loopIndex == loopCount):#相等的话则执行最后一次不满BaseLoop的遍历
            BaseLoop = last #遍历剩下的全部
        sql = 'select * from %s limit %d,%d' %(table,start,BaseLoop)
        # sql = 'select * from %s limit 0,5000' %(table)
        print('sql:',loopIndex,sql)
        cur.execute(sql)
        data = cur.fetchall()
        jsonData = []  
        for row in data: #遍历所有数据
            result = {}    # temp store one jsonObject
            fieldIndex = 0
            for field in rowItem:#转换数据格式为json
                result[field] = row[fieldIndex]
                fieldIndex += 1
            tt = json.dumps(result,ensure_ascii=False,cls=DateEncoder)
            # print('aaa',tt)
            if(config.exportMongo):#如果是导出为mongoexport json格式
                f.write(tt)
                f.write('\n')
            else:
                jsonData.append(tt)
            # print('result',result)
        loopIndex += 1#
        if(not config.exportMongo):
            print('result')
            f.write(json.dumps(jsonData,ensure_ascii=False))
    f.close()
    return

class DateEncoder(json.JSONEncoder ):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.__str__()  
        return json.JSONEncoder.default(self, obj)  
 

if __name__ == '__main__':  
    tableToJson(tableName) 
    cur.close()
    conn.close() 
     
    
    