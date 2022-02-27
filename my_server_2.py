# server.py
import http.server # Our http server handler for http requests
import socketserver # Establish the TCP Socket connections
import operator
import pymysql
import json

PORT = 8090

conn = pymysql.connect("localhost", "root", "mypassword", "localdata")

class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    def write_response(self, jsons):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(bytes(json.dumps(jsons), 'utf-8'))

    def do_GET(self):
        self.path = 'index.html'
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        obj = json.loads(post_data.decode())
        print(obj)
        latitude = obj["latitude"]
        longitude = obj["longitude"]
        
        cur = conn.cursor()
        sql = """SELECT * FROM `topic`"""
        cur.execute(sql)
        rows = cur.fetchall()
        
        objs=[]
        for row in rows:
            obj = {"title": row[2], "content":row[3], "latitude":row[4], "longitude":row[5]}
            objs.append(obj)
        
        # self.send_response(200)
        # self.send_header('Content-type', 'text/html')
        # self.end_headers()
        # self.wfile.write(bytes(json.dumps(objs), 'utf-8'))
        self.write_response(objs)
        
        #return http.server.SimpleHTTPRequestHandler.do_GET(self)

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        obj = json.loads(post_data.decode())
        print(obj)
        cur = conn.cursor()
        
        # self.send_response(200)
        # self.send_header('Content-type', 'text/html')
        # self.end_headers()
        
        if obj["type"] == "add":
            topic = (obj["user_id"], obj["title"], obj["content"], obj["latitude"], obj["longitude"])
            cur.execute('insert into topic(user_id, title, content, lat, log) values(%s, %s, %s, %s, %s)', topic)
            conn.commit()

           # self.wfile.write(bytes(json.dumps({'status':'ok'}), 'utf-8'))
            self.write_response({'status':'ok'})

        elif obj["type"] == "get":
            latitude = obj["latitude"]
            longitude = obj["longitude"]
            keyword = obj["keyword"]
            if keyword:
                sql = """SELECT *, ((ACOS(SIN(%s * PI() / 180) * SIN(lat * PI() / 180) + COS(%s * PI() / 180) * COS(lat * PI() / 180) * COS((%s - log) * PI() / 180)) * 180 / PI()) * 60 * 1.1515 * 1.609344) as distance FROM topic HAVING distance <= 3 and (title like concat('%%', %s, '%%') or content like concat('%%', %s, '%%')) ORDER BY distance limit 50"""
                cur.execute(sql, (latitude, latitude, longitude, keyword, keyword))
            else:
                sql = """SELECT *, ((ACOS(SIN(%s * PI() / 180) * SIN(lat * PI() / 180) + COS(%s * PI() / 180) * COS(lat * PI() / 180) * COS((%s - log) * PI() / 180)) * 180 / PI()) * 60 * 1.1515 * 1.609344) as distance FROM topic HAVING distance <= 3 ORDER BY distance limit 50"""
                cur.execute(sql, (latitude, latitude, longitude))
            rows = cur.fetchall()
            tmp_objs=[]
            for row in rows:
                cur.execute('select * from reply where topic_id = %s', row[0])
                results = cur.fetchall()
                replies=[]
                for r in results:
                    replies.append(r[3])
                obj2 = {"topicId": row[0], "title": row[2], "content":row[3], "latitude":row[4], "longitude":row[5], "replies": replies, "hotness": len(replies)}
                tmp_objs.append(obj2)
            tmp_objs.sort(key=operator.itemgetter('hotness'), reverse=True)
            objs = []
            for obj in tmp_objs:
                obj2 = {"topicId": obj['topicId'], "title": obj['title'], "content":obj['content'], "latitude":obj['latitude'], "longitude":obj['longitude'], "replies": obj['replies']}
                objs.append(obj2)
            #self.wfile.write(bytes(json.dumps({'topics':objs}), 'utf-8'))
            self.write_response({'topics':objs})
        elif obj["type"] == "fun":
            latitude = obj["latitude"]
            longitude = obj["longitude"]
            sql = """SELECT *, ((ACOS(SIN(%s * PI() / 180) * SIN(lat * PI() / 180) + COS(%s * PI() / 180) * COS(lat * PI() / 180) * COS((%s - log) * PI() / 180)) * 180 / PI()) * 60 * 1.1515 * 1.609344) as distance FROM fun HAVING distance <= 10 ORDER BY distance limit 50"""
            cur.execute(sql, (latitude, latitude, longitude))
            rows = cur.fetchall()
            objs=[]
            for row in rows:
                obj2 = {"name": row[1], "description": row[2], "latitude":row[3], "longitude":row[4]}
                objs.append(obj2)
            #self.wfile.write(bytes(json.dumps({'funs':objs}), 'utf-8'))
            self.write_response({'funs':objs})
        elif obj["type"] == "reply":
            topic_id = obj["topic_id"]
            reply = obj["content"]
            cur.execute('insert into reply(topic_id, content) values(%s, %s)', (topic_id, reply))
            conn.commit()
            #self.path = 'index.html'
            #self.wfile.write(bytes(json.dumps({'status':'ok'}), 'utf-8'))
            self.write_response({'status':'ok'})
        elif obj["type"] == "topic":
            topic_id = obj["topic_id"]
            cur.execute('select * from topic where topic_id = %s', topic_id)
            rows = cur.fetchall()
            topic = rows[0]
            cur.execute('select * from reply where topic_id = %s', topic_id)
            results = cur.fetchall()
            replies=[]
            for r in results:
                replies.append(r[3])
            obj2 = {"topicId": topic[0], "title": topic[2], "content":topic[3], "latitude":topic[4], "longitude":topic[5], "replies": replies}
            #self.wfile.write(bytes(json.dumps(obj2), 'utf-8'))
            self.write_response(obj2)
        else:
            print("not implemented")

Handler = MyHttpRequestHandler
 
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print("Http Server Serving at port", PORT)
    httpd.serve_forever()
