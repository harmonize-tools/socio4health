import scrapy
import json
import os
import logging

class StandardSpider(scrapy.Spider):
    name = 'standard'

    def __init__(self, url=None, depth=0, ext=['.csv','.xls','.xlsx','.zip'], *args, **kwargs):
        super().__init__(*args, **kwargs)
        if url is None:
            logging.warning("No URL provided. Please specify a URL.")
            return
        self.start_urls = [url]
        self.depth = depth
        self.links = {}
        self.ext = ext

    def parse(self, response, current_depth=0):
        if current_depth <= self.depth:
            # Process elements with the class "resource"
            try:
                elementos = response.css('.resource')
                for elemento in elementos:
                    enlace = elemento.css('a::attr(href)').extract_first()
                    if enlace is None:
                        enlace = elemento.css('input[type="image"]::attr(onclick)').re_first(r"'(https://[^']+)'").replace(" ","")
                        nombre_archivo = elemento.css('input[type="image"]::attr(title)').extract_first()
                    
                    if enlace:
                        nombre_archivo = elemento.css('input[type="image"]::attr(title)').extract_first()
                        if nombre_archivo:
                            # Modify the file name by removing the first three and last three characters
                            
                            modified_nombre_archivo = nombre_archivo
                            full_url = response.urljoin(enlace)
                            self.links[modified_nombre_archivo] = full_url

                # Continue the existing functionality for checking links and their extensions
                elementos = response.css('a::attr(href)').getall()
                for enlace in elementos:
                    full_url = response.urljoin(enlace)
                    if any(enlace.endswith(extension) for extension in self.ext):
                        nombre_archivo = os.path.basename(enlace)
                        self.links[nombre_archivo] = full_url
                    elif current_depth < self.depth:
                        # Follow the link if it's not a file and within depth limit
                        yield response.follow(enlace, self.parse2, cb_kwargs={'current_depth': current_depth + 1})
                print(f"Successfully ran spider") 
            except:
                print("Spider failed, trying to get the direct source...")


        # Log or print the links (you can also process them as needed)
        #for name, link in self.links.items():
        #    self.log(f"Found file: {name} at {link}")

    def closed(self, reason):
        # Optional: save the links to a JSON file
        with open('Output_scrap.json', 'w') as file:
            json.dump(self.links, file, indent=4)
