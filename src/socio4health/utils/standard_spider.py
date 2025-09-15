import re
import scrapy
import json
import os
import logging
from scrapy.exceptions import IgnoreRequest
import copy


class StandardSpider(scrapy.Spider):
    """A standard spider for scraping links from a given ``URL``.

    Attributes
    ----------
    url : str, optional
        The URL to start scraping from. If not provided, a warning is logged.
    depth : int, optional
        The maximum depth to follow links. Default is 0 (no depth limit).
    ext : list, optional
        A list of file extensions to filter links. Default includes common document formats.
    key_words : list, optional
        A list of keywords or regex conditions to filter links by filename. By default it is an empty list.
    start_urls : list
        A list containing the starting URL for the spider.
    links : dict
        A dictionary to store found links with filenames as keys and URLs as values.
    name : str
        The name of the spider, used for identification in logs and output.


    """
    name = 'standard'

    def __init__(self, url=None, depth=0, ext=None, key_words=None, *args, **kwargs):
        """Initialize the spider with parameters."""
        super().__init__(*args, **kwargs)
        if url is None:
            logging.warning("No URL provided. Please specify a URL.")
            return
        self.start_urls = [url]
        self.depth = depth
        self.links = {}
        self.ext = ext if ext is not None else ['.csv', '.xls', '.xlsx', '.zip']
        self.key_words = key_words if key_words is not None else []

        if self.key_words and not hasattr(self, "_compiled_key_words"):
            self._compiled_key_words = []
            for kw in self.key_words:
                try:
                    # If it is a valid regex, it compiles as is.
                    self._compiled_key_words.append(re.compile(kw))
                except re.error:
                    # If it is NOT a valid regex, it treats it as a literal.
                    self._compiled_key_words.append(re.compile(re.escape(kw)))

    def parse(self, response, current_depth=0):
        """Parse the response to extract links based on criteria.

        Parameters
        ----------
        response : scrapy.http.Response
            The response object containing the ``HTML`` content of the page.
        current_depth : int, optional
            The current depth of the link being processed. Default is 0.


        """
        if current_depth <= self.depth:
            # Check if the response content is text (HTML)
            content_type = response.headers.get('Content-Type', b'').decode('utf-8')
            if 'text/html' not in content_type:
                self.logger.warning(f"Skipping non-text response: {response.url} (Content-Type: {content_type})")
                return  # Skip non-text responses

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
                            if any(p.search(nombre_archivo) for p in self._compiled_key_words):
                                self.links[nombre_archivo] = full_url
                        else:
                            self.links[nombre_archivo] = full_url
                    elif 'title' in elemento.attrib:
                        for extension in self.ext:
                            if elemento.attrib['title'].endswith(extension):
                                nombre_archivo = os.path.basename(enlace + extension)
                                if self.key_words:  # Ensure key_words is not empty or None
                                    if any(p.search(nombre_archivo) for p in self._compiled_key_words):
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
                                if any(p.search(nombre_archivo) for p in self._compiled_key_words):
                                    self.links[nombre_archivo] = response.urljoin(onclick_url)
                            else:
                                self.links[nombre_archivo] = response.urljoin(onclick_url)

            except IgnoreRequest:
                self.logger.warning("Request ignored due to robots.txt restriction.")
            except Exception as e:
                self.logger.error(f"Spider failed due to an error: {e}", exc_info=True)

    def closed(self, reason):
        """Handle actions to perform when the spider is closed.
        Parameters
        ----------
        reason : str
        The reason for closing the spider, e.g., 'finished', 'shutdown', etc.

        Notes
        -----
        This method saves the collected links to a JSON file.

        """


        output_file = 'Output_scrap.json'  # Consider making this dynamic by passing as an argument
        with open(output_file, 'w') as file:
            json.dump(self.links, file, indent=4)
            self.logger.info(f"Successfully saved links to {output_file}.")
