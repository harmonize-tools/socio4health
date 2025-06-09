import scrapy
import json
import os
import logging
from scrapy.exceptions import IgnoreRequest
import copy

class StandardSpider(scrapy.Spider):
    name = 'standard'

    def __init__(self, urls=None, depth=0, ext=['.csv','.xls','.xlsx','.zip'], key_words=[], *args, **kwargs):
        super().__init__(*args, **kwargs)
        if urls is None:
            logging.warning("No URL provided. Please specify a URL.")
            return
        self.start_urls = urls
        self.depth = depth
        self.links = {}
        self.ext = ext
        self.key_words = key_words

    def parse(self, response, current_depth=0):
        if current_depth <= self.depth:
            try:
                # Broad search for links in <a>, <link>, and <area> tags.
                elementos = response.css('a[href], link[href], area[href]')
                for elemento in elementos:
                    enlace = elemento.attrib['href']
                    full_url = response.urljoin(enlace)
                    if "Registro-de-activos-de-informacion" in full_url:
                        continue
                    if any(enlace.endswith(extension) for extension in self.ext):
                        nombre_archivo = os.path.basename(enlace)
                        if self.key_words:  # Ensure key_words is not empty or None
                            if any(key_word in nombre_archivo for key_word in self.key_words):
                                self.links[nombre_archivo] = full_url
                        else:
                            self.links[nombre_archivo] = full_url
                    elif current_depth < self.depth:
                        yield response.follow(enlace, self.parse, cb_kwargs={'current_depth': current_depth + 1})

                # Specific search for <input type="image"> elements.
                image_inputs = response.css('input[type="image"]')
                for input_element in image_inputs:
                    onclick_url = input_element.css('::attr(onclick)').re_first(r"'(https://[^']+)'")
                    if onclick_url:
                        onclick_url = onclick_url.replace(" ", "")
                        nombre_archivo = copy.deepcopy(input_element.css('::attr(title)').extract_first())
                        if nombre_archivo:
                            if self.key_words:  # Ensure key_words is not empty or None
                                if any(key_word in nombre_archivo for key_word in self.key_words):
                                    self.links[nombre_archivo] = response.urljoin(onclick_url)
                            else:
                                 self.links[nombre_archivo] = response.urljoin(onclick_url)


            except IgnoreRequest:
                self.logger.warning("Request ignored due to robots.txt restriction.")
            except Exception as e:
                self.logger.error(f"Spider failed due to an error: {e}", exc_info=True)

    def closed(self, reason):
        # Optional: save the links to a JSON file
        with open('Output_scrap.json', 'w') as file:
            json.dump(self.links, file, indent=4)
            print(f"Successfully ran spider")
