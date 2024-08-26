#!/usr/bin/env python3

from bs4 import BeautifulSoup

with open("target/site/jacoco-aggregate/index.html", mode='r') as html_file:
    html_doc = html_file.read()
    soup = BeautifulSoup(html_doc, 'html.parser')
    print(soup.find(id="coveragetable").find("tfoot").find("td", class_="ctr2").text)
