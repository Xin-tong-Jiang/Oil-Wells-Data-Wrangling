from flask import Flask, jsonify, send_from_directory
import mysql.connector
import os

project_dir = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_url_path='', static_folder=os.path.join(project_dir, 'static'))

# -------------------- DB Config --------------------
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "phpmyadmin")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "oilwell_pdf_extraction")

@app.route("/wells")
def wells():
    conn = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
    )
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT wi.*, ws.*
        FROM well_info wi
        LEFT JOIN well_stimulation ws ON wi.pdf_name = ws.pdf_name
        WHERE 
            wi.latitude IS NOT NULL
            AND wi.longitude IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@app.route('/')
def home():
    return send_from_directory('static', 'index.html')

@app.route('/map')
def map_page():
    return send_from_directory('static', 'map.html')
