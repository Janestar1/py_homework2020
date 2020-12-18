import datetime
from aiohttp import web
from dataclasses import asdict
from serv.json_util import json_dumps

from .config import db_block, web_routes


@web_routes.get("/api/course/list")
async def get_course_list(request):
    with db_block() as db:
        db.execute("""
        SELECT sn AS cou_sn, no AS cou_no, name AS cou_name, teacher, term, room, week, day, jie FROM course
        """)
        data = list(asdict(r) for r in db)
        
    return web.Response(text=json_dumps(data), content_type="application/json")


@web_routes.get("/api/course/{cou_sn:\d+}")
async def get_course_profile(request):
    cou_sn = request.match_info.get("cou_sn")

#用字典类型传递参数（优点：易读，且不易搞错位置）
    with db_block() as db:#SQL语句中，参数采用%(key)s语法格式
        db.execute("""
        SELECT sn AS cou_sn, no AS cou_no, name AS cou_name, teacher, term, room, week, day, jie FROM course
        WHERE sn=%(cou_sn)s
        """, dict(cou_sn=cou_sn))
        record = db.fetch_first()

    if record is None:
        return web.HTTPNotFound(text=f"no such course: cou_sn={cou_sn}")

    data = asdict(record)
    return web.Response(text=json_dumps(data), content_type="application/json")


@web_routes.post("/api/course")
async def new_course(request):
    course = await request.json()
  #  if not course.get('enrolled'):
  #     course['enrolled'] = datetime.date( Mon,1, 1)

    with db_block() as db:
        db.execute("""
        INSERT INTO course (no, name,teacher, term, room, week, day, jie)
        VALUES(%(cou_no)s, %(cou_name)s, %(teacher)s, %(term)s, %(room)s, %(week)s, %(day)s, %(jie)s) RETURNING sn;
        """,course)
        record = db.fetch_first()

        course["cou_sn"] = record.sn
    
    print(course)

    return web.Response(text=json_dumps(course), content_type="application/json")


@web_routes.put("/api/course/{cou_sn:\d+}")
async def update_course(request):
    cou_sn = request.match_info.get("cou_sn")

    course = await request.json()
    #if not course.get('enrolled'):
    #    course['enrolled'] = datetime.date(Mon ,1, 1)

    course["cou_sn"] = cou_sn

    with db_block() as db:
        db.execute("""
        UPDATE course SET
            no=%(cou_no)s, name=%(cou_name)s, teacher = %(teacher)s, term = %(term)s, room = %(room)s, week = %(week)s, day = %(day)s, jie = %(jie)s
        WHERE sn=%(cou_sn)s;
        """, course)

    return web.Response(text=json_dumps(course), content_type="application/json")


@web_routes.delete("/api/course/{cou_sn:\d+}")
async def delete_course(request):
    cou_sn = request.match_info.get("cou_sn")

    with db_block() as db:
        db.execute("""
        DELETE FROM course WHERE sn=%(cou_sn)s;
        """, dict(cou_sn=cou_sn))

@web_routes.put("/api/course/{cou_sn:\d+}")
async def query_course(request):
    cou_sn = request.match_info.get("cou_sn")

    course = await request.json()

    course["cou_sn"] = cou_sn

    with db_block() as db:
        db.execute("""
        SELECT *
        FROM sc
        WHERE student_sn IN (
            SELECT student_sn
            FROM sc,course
            WHERE sc.cou_sn=course.no AND course.name=%(cou_name)s AND course.term= %(term)s)

        """, course)

    return web.Response(text=json_dumps(course), content_type="application/json")

    return web.Response(text="", content_type="text/plain")
