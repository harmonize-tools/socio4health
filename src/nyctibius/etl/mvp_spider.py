import scrapy
import json


class MiSpi1der(scrapy.Spider):
    name = 'mvp'
    start_urls = ['https://microdatos.dane.gov.co/index.php/catalog/643/get_microdata']
    #start_urls = input("Ingrese el link")
    links = {}  # Diccionario para almacenar los enlaces de Excel por enlace inicial

    def parse(self, response):
        # Encuentra todos los elementos con la clase "resource"
        elementos = response.css('.resource')

        for elemento in elementos:
            # Extrae el enlace dentro del elemento
            enlace = elemento.css('a::attr(href)').extract_first()

            # Si no se encontró un enlace directo, busca dentro del elemento para encontrar el enlace
            if enlace is None:
                enlace = elemento.css('input[type="image"]::attr(onclick)').re_first(r"'(https://[^']+)'")

            if enlace:
                # Extrae el nombre del archivo desde el atributo "title" del enlace
                nombre_archivo = elemento.css('input[type="image"]::attr(title)').extract_first()
                print(nombre_archivo)
                if nombre_archivo:
                    # Agrega el enlace al diccionario usando el nombre del archivo como clave
                    self.links[nombre_archivo[3:-3]] = enlace

    def closed(self, reason):
        # Guarda el diccionario en un archivo JSON
        with open('enlaces_vivienda.json', 'w') as archivo_json:
            json.dump(self.links, archivo_json, indent=4)

        self.log(f'Se han encontrado enlaces y se han guardado en enlaces.json')