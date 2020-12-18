from aiohttp import web
from .config import db_block, web_routes, render_html


@web_routes.get("/sc")
async def view_list_sc(request):
    with db_block() as db:
        db.execute("""
        SELECT sn AS stu_sn, name as stu_name FROM student ORDER BY name
        """)
        students = list(db)

        db.execute("""
        SELECT sn AS cou_sn, name as cou_name, term as cou_term, week as cou_week,
        day as cou_day, jie as cou_jie FROM course ORDER BY name
        """)
        courses = list(db)

        db.execute("""
        SELECT stu_sn, cou_sn, state FROM sc ORDER BY stu_sn
        """)
        sc = list(db)


        db.execute("""
        SELECT sc1.stu_sn, sc1.cou_sn, 
            s.name as stu_name, 
            c.name as cou_name,
            c.term as cou_term, 
            c.week as cou_week,
            c.day as cou_day,
            c.jie as cou_jie
        FROM sc as sc1
            INNER JOIN student as s ON sc1.stu_sn = s.sn
            INNER JOIN course as c  ON sc1.cou_sn = c.sn
        ORDER BY stu_sn, cou_sn;
        """)

        items = list(db)

    return render_html(request, 'sc_list.html',
                       students=students,
                       courses=courses,
                       sc=sc,
                       items=items)


@web_routes.get('/sc/edit/{stu_sn}/{cou_sn}')
def view_sc_editor(request):
    stu_sn = request.match_info.get("stu_sn")
    cou_sn = request.match_info.get("cou_sn")
    state = request.match_info.get("state")
    if stu_sn is None or cou_sn is None:
        return web.HTTPBadRequest(text="stu_sn, cou_sn, must be required")



    with db_block() as db:
        db.execute("""
        SELECT state FROM sc
            WHERE stu_sn = %(stu_sn)s AND cou_sn = %(cou_sn)s;
        """, dict(stu_sn=stu_sn, cou_sn=cou_sn))

        record = db.fetch_first()

        db.execute("""
        SELECT sn AS cou_sn, name as cou_name, term as cou_term, week as cou_week,
        day as cou_day, jie as cou_jie FROM course ORDER BY name
        """)
        courses = list(db)


        db.execute("""
        SELECT sc1.stu_sn, sc1.cou_sn, 
            s.name as stu_name, 
            c.name as cou_name,
            c.term as cou_term, 
            c.week as cou_week,
            c.day as cou_day,
            c.jie as cou_jie
        FROM sc as sc1
            INNER JOIN student as s ON sc1.stu_sn = s.sn
            INNER JOIN course as c  ON sc1.cou_sn = c.sn
        ORDER BY stu_sn, cou_sn;
        """)

        sc_cou = list(db)


    if record is None:
        return web.HTTPNotFound(text=f"no such sc: stu_sn={stu_sn}, cou_sn={cou_sn}, cou_name={cou_name},cou_term={cou_term}")

    return render_html(request, "sc_edit.html",
                       stu_sn=stu_sn,
                       cou_sn=cou_sn,
                       courses=courses,
                       sc_cou=sc_cou,
                       state=record.state)


@web_routes.get("/sc/delete/{stu_sn}/{cou_sn}")
def sc_deletion_dialog(request):
    stu_sn = request.match_info.get("stu_sn")
    cou_sn = request.match_info.get("cou_sn")
    if stu_sn is None or cou_sn is None:
        return web.HTTPBadRequest(text="stu_sn, cou_sn, must be required")

    with db_block() as db:
        db.execute("""
        SELECT sc1.stu_sn, sc1.cou_sn,
            s.name as stu_name, 
            c.name as cou_name, 
            sc1.state
        FROM sc as sc1
            INNER JOIN student as s ON sc1.stu_sn = s.sn
            INNER JOIN course as c  ON sc1.cou_sn = c.sn
        WHERE stu_sn = %(stu_sn)s AND cou_sn = %(cou_sn)s;
        """, dict(stu_sn=stu_sn, cou_sn=cou_sn))

        record = db.fetch_first()

    if record is None:
        return web.HTTPNotFound(text=f"no such sc: stu_sn={stu_sn}, cou_sn={cou_sn}")

    return render_html(request, 'sc_dialog_deletion.html', record=record)
