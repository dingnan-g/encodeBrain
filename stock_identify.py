# ---------------------------------------------------------------------
# 基金自动勾兑
# ---------------------------------------------------------------------
#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""开发人员:丁楠
create_date:20200815
update_date:20201018"""
import pandas as pd
import numpy as np
import cx_Oracle
import jieba
import jieba.analyse as p
import re
import csv
import os
import gc

#---连接数据库---

db=cx_Oracle.connect('xxxxx','xxxxx','172.22.135.75:1521/biee');
cr=db.cursor();

# ---清除临时表和结果表数据---
sql3="delete from xxxx.TSS_SEAT_RECEIPTS_PROD";
cr.execute(sql3);
db.commit();
#---获取股销实收数据---
sql1="select id,stat_dt,abstract,OPPOSITE_ACCT_NAME,BILL_NAME from xxxx.TSS_SEAT_INCOME_RECEIPTS_MID group by id,stat_dt,abstract,OPPOSITE_ACCT_NAME,BILL_NAME";
cr.execute(sql1);
rs=cr.fetchall();
df1=pd.DataFrame(rs);
trade_seat=[];
corp_name=[];

data_sets_ori=pd.DataFrame({'stat_dt':df1[1].tolist(),'abstract':df1[2].tolist(),'opposite_acct_name':df1[3].tolist(),'bill_name':df1[4].tolist()});
data_sets=np.array(data_sets_ori);
data_set_list=data_sets.tolist();
df1.rename(columns={0 :'id', 1 :'abstract', 2 : 'opposite_acct_name',3:'bill_name'}, inplace=True);
id=[];
id= df1['id'];

#---定义提取实收相关信息的类和方法
class GetData_Receipt_Info:
      # ---提取实收产品名称---
      def GetCorpName(self):
          for i in range(len(data_set_list)):
              data=str(data_set_list[i][1])
              # reg = "[^0-9A-Za-z\u4e00-\u9fa5]"
              # data=re.sub(reg,'',data)
              result = re.findall(u'[\u4e00-\u9fa5]',data)                                                                               
              data = ''.join(result) 
      # ---取产品名称---
              data2=str(data_set_list[i][2])
              data3=str(data_set_list[i][3])
              if data2.find(u"中国工商银行") == -1:
                 reg = "[交行托管专户\兴业银行资产托管专户\兴业银资产\托管专户]"
                 # reg = "[交行托管专户\兴业银资产\托管专户\汇添富基金理股份有限公司\博时基金管理有限公司\嘉实基金管理有限公司－\天弘基金管理有限公司－\中信证券股份有限公司－\富国基金管理有限公司－\银华基金管理股份有限公司－\大成基金管理有限公司－]"
                 data2=re.sub(reg,'',data2)
                 a=r'\(.*\)'
                 data2 = re.sub(a,'',data2)
                 # data2=data2.replace(' ','')
                 # data2=data2.lstrip()
                 # data2=data2.rstrip()
                 # data2=data2.strip()
                 data2=data2.replace("投基金",'投资基金')
                 data2=data2.replace("集合",'集合资产管理')
                 data2=data2.replace("（",'(')
                 data2=data2.replace("）",')')
                 data2=data2.replace("ＬＯＦ",'LOF')
                 data2=data2.replace("工瑞信",'工银瑞信')
                 corp_name.append(data2)
              else:
                  stopwords= ('佣','佣金','支','付','截','止','至','年','月','付','席','位','日','第','季','度','金','划','的','交','易','单','元','应')
                  final=" "
                  for word in data:
                      if word not in stopwords:
                          if (word!="-" and word!="." and word!="," and word!=" " and word!="。"):
                              if not word in final:
                                  final = final + word
                  final=final.replace("启",'启元')
                  if final is None or final==" ":
                      corp_name.append(data3)
                  else:
                      corp_name.append(final)
          return corp_name
      
      # ---提取实收交易席位---
      def GetTradeSeat(self):
          for i in range(len(data_set_list)):
              data=str(data_set_list[i][1])
              # reg = "[^0-9A-Za-z\u4e00-\u9fa5]"
              # data=re.sub(reg,'',data)
              tidf=p.extract_tags
              keywords=tidf(data)
              seat=" "
      # ----确定提取关键词后是否为空,针对特殊文本---
              if len(keywords)!= 0:
                 for keyword in keywords:
                     if keyword.isdigit():
                         if keyword[:4] not in ('2015','2016','2017','2018','2019','2020','2021','2022','2023','2024','2025'):
                            if len(keyword)==4:
                               keyword='00'+keyword
                               seat= seat + keyword + ','
                            else: 
                                 seat= seat + keyword + ','
                 if seat[-1] == ',':
                    trade_seat.append(seat[:-1])
                 else: 
                      trade_seat.append(seat)
              else:
                   seat=int(data)
                   seat=str(seat)
                   if len(seat)==4:
                      seat='00'+ seat
                   trade_seat.append(seat)
          return trade_seat

class Identify_Prod_Seat_Info:
      # 将识别的产品名称和交易席位入库
      def IdentifyData(id,corp_name,trade_seat):
          #------将提取的实收数据信息合并为DataFrame,并存入库表中                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
          data_recipt=pd.DataFrame({'id':id,'prod_name':corp_name,'trade_seat':trade_seat})
          tmp_list1=np.array(data_recipt).tolist()

          #准备sql串,形如 insert into table_name(a,b,c) values(:a,:b,:c)
          sql_string1='insert into {}({}) values({})'.format('xxxxx.TSS_SEAT_RECEIPTS_PROD',','.join(list(data_recipt.columns)),','.join(list(map(lambda x:':'+x ,data_recipt.columns))))
          cr.executemany(sql_string1,tmp_list1)
          db.commit()
          
if __name__ == '__main__':

    corp_name=GetData_Receipt_Info.GetCorpName(data_set_list)
    trade_seat=GetData_Receipt_Info.GetTradeSeat(data_set_list)
    Identify_Prod_Seat_Info.IdentifyData(id,corp_name,trade_seat)
    gc.collect()


