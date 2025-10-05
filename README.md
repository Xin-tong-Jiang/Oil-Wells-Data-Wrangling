# Oil Wells Data Wrangling

This project focuses on text extraction, web scraping, data processing, and visualization from scanned PDF files.

## Features
- Data Collection / Storage
- PDF Extraction
- Additional Web Scraped Information
- Web Access and Visualization

## Usage

Create and activate a virtual environment:  
`python -m venv venv`  
`source venv/bin/activate`  

Install dependencies:  
`pip install -r requirements.txt`  

Put database login info in the same directory:  
`.env`  

Run pdf extraction script (replace database configuration with your own):  
`python pdf_extraction.py DSCI560_Lab5`

`python pdf_to_db.py --header well_header.csv --stim well_stimulation.csv`

## Map Webapp

This project includes a simple web application to visualize oil well locations on a map. The backend is a Flask app that serves well data from a MySQL database. The frontend uses Leaflet to render the map and markers. Apache is used as a web server and reverse proxy to serve the Flask app via uWSGI. Static files (HTML, JS, CSS, libraries) are served from the `/static` folder.

### Apache Configuration Template

Below is an example Apache virtual host configuration. Replace paths, user, and group as needed for your environment.

```apache
<VirtualHost *:80>
    ServerName localhost

    WSGIDaemonProcess oilwells python-home=/path/to/project/venv python-path=/path/to/project
    WSGIScriptAlias / /path/to/project/app.wsgi

    <Directory /path/to/project>
        Require all granted
        Options +FollowSymLinks
        AllowOverride All
    </Directory>

    ErrorLog ${APACHE_LOG_DIR}/oilwells_error.log
    CustomLog ${APACHE_LOG_DIR}/oilwells_access.log combined
</VirtualHost>

