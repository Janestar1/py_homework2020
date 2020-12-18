from aiohttp import web
import psycopg2.errors
from urllib.parse import urlencode

from .config import db_block, web_routes

import datetime
from dataclasses import asdict
from serv.json_util import json_dumps


@web_routes.post('/action/sc/add')
async def action_sc_add(request):

    paramss = await request.post()
    stu_sn = paramss.get("stu_sn")
    cou_sn = paramss.get("cou_sn")
    state = paramss.get("state")

    if stu_sn is None or cou_sn is None or state is None:
        return web.HTTPBadRequest(text="stu_sn, cou_sn, sc must be required")

    try:
        stu_sn = int(stu_sn)
        cou_sn = int(cou_sn)
    except ValueError:
        return web.HTTPBadRequest(text="stu_sn, cou_sn, sc must must exist")

    try:
        with db_block() as db:
            db.execute("""
            INSERT INTO sc (stu_sn, cou_sn, state) 
            VALUES ( %(stu_sn)s, %(cou_sn)s, %(state)s)
            """, dict(stu_sn=stu_sn, cou_sn=cou_sn, state=state))
            
            # record = db.fetch_first()
            # sc["sc_sn"] = record.sn

    except psycopg2.errors.UniqueViolation:
        query = urlencode({
            "message": "已经添加该学生的课程",
            "return": "/sc"
        })
        return web.HTTPFound(location=f"/error?{query}")
    except psycopg2.errors.ForeignKeyViolation as ex:
        return web.HTTPBadRequest(text=f"无此学生或课程: {ex}")

    return web.HTTPFound(location="/sc")


@web_routes.post('/action/sc/edit/{stu_sn}/{cou_sn}')
async def edit_sc_action(request):
    stu_sn = request.match_info.get("stu_sn")
    cou_sn = request.match_info.get("cou_sn")
    if stu_sn is None or cou_sn is None:
        return web.HTTPBadRequest(text="stu_sn, cou_sn, must be required")

    paramss = await request.post()
    cou_sn = paramss.get("cou_sn")

    try:
        stu_sn = int(stu_sn)
        cou_sn = int(cou_sn)
    except ValueError:
        return web.HTTPBadRequest(text="invalid value")

    with db_block() as db:
        db.execute("""
        UPDATE sc SET cou_sn=%(cou_sn)s
        WHERE stu_sn = %(stu_sn)s
        """, dict(stu_sn=stu_sn, cou_sn=cou_sn))

    return web.HTTPFound(location="/sc")


@web_routes.post('/action/sc/delete/{stu_sn}/{cou_sn}')
def delete_sc_action(request):
    stu_sn = request.match_info.get("stu_sn")
    cou_sn = request.match_info.get("cou_sn")
    if stu_sn is None or cou_sn is None:
        return web.HTTPBadRequest(text="stu_sn, cou_sn, must be required")

    with db_block() as db:
        db.execute("""
        DELETE FROM sc
            WHERE stu_sn = %(stu_sn)s AND cou_sn = %(cou_sn)s
        """, dict(stu_sn=stu_sn, cou_sn=cou_sn))

    return web.HTTPFound(location="/sc")
