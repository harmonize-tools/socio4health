import scrapy
import json
import os
import logging

class StandardSpider(scrapy.Spider):
    name = 'standard'

    def __init__(self, url=None, depth=0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if url is None:
            logging.warning("No URL provided. Please specify a URL.")
            return
        self.start_urls = [url]
        self.depth = depth
        self.links = {}

    def parse(self, response, current_depth=0):
        # Check if the current depth is less than or equal to the allowed depth
        if current_depth <= self.depth:
            elementos = response.css('a::attr(href)').getall()

            for enlace in elementos:
                full_url = response.urljoin(enlace)
                if enlace.endswith('.csv') or enlace.endswith('.xls') or enlace.endswith('.xlsx'):
                    nombre_archivo = os.path.basename(enlace)
                    self.links[nombre_archivo] = full_url
                elif current_depth < self.depth:
                    # Follow the link if it's not a file and within depth limit
                    yield response.follow(enlace, self.parse, cb_kwargs={'current_depth': current_depth + 1})

        # Log or print the links (you can also process them as needed)
        for name, link in self.links.items():
            self.log(f"Found file: {name} at {link}")

    def closed(self, reason):
        # Optional: save the links to a JSON file
        with open('Output_scrap.json', 'w') as file:
            json.dump(self.links, file, indent=4)
        self.log(f'Links found and saved in enlaces_archivos.json')
