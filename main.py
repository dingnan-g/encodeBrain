# -*-coding:utf-8 -*-
import os
from bottle import route,run,request


@route('/identify', method='POST')
def identify():
    data=request.body.readlines()
    print('data---' + str(data))
    os.system("python C:\\goudui\\stock_identify.py")
    if True:
       return {"success":True, "msg":"执行成功"}
    else:
        return {"success":False, "msg":"执行失败,存在问题"}

run(host='0.0.0.0', port=88)