# Oil Wells Data Wrangling

This project focuses on text extraction, web scraping, data processing, and visualization from scanned PDF files.

## Features
Data Collection / Storage
PDF Extraction
Additional Web Scraped Information
Web Access and Visualization

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
