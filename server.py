from aiohttp import web

from serv.config import web_routes, home_path

import serv.error_views
import serv.main_views
import serv.grade_views
import serv.grade_actions
import serv.student_views
import serv.student_rest
import serv.course_views
import serv.course_rest
import serv.sc_views
import serv.sc_rest



app = web.Application()
app.add_routes(web_routes)
app.add_routes([web.static("/", home_path / "static")])

if __name__ == "__main__":
    web.run_app(app, port=8080)
