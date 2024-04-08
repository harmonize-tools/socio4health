import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
import os
from pandasai import Agent
from pandasai import Agent
import pandas as pd
import os
import json
from .db.modifier import Modifier
from pandasai.connectors import SqliteConnector, PandasConnector
from pandasai.llm import OpenAI
import warnings
import logging

class BirdAgent(Agent):

    def __init__(
        self,
        dfs="data\\output\\nyctibius.db",
        config:dict=None,
        memory_size=10,
        pipeline=None,
        vectorstore=None,
        description=None,
    ):
        self.memory = {}
        self.counter = 0
        self.dfs = dfs
        self.dfs = self.prepare_data()
        self.config = self.prepare_config()
        super().__init__(self.dfs,self.config,memory_size,pipeline,vectorstore,description)  
        logging.getLogger('pandasai').propagate = False

    def generate_code(self, prompt:str, output_type=None):
        self.last_code_generated = super().generate_code(prompt, output_type)
        self.last_prompt = prompt
        self.new_memory_entry(service="gen_code", prompt=prompt, code=self.last_code_generated, result=None)
        return self.last_code_generated
    
    def execute_code(self, code=None, output_type=None):
        if not code:
            code = self.last_code_generated
        if not code:
            code = self.last_code_executed
        if not code:
            code = self.memory[f"chat_{self.counter-1}"]["code"]
        if "plt" in code:
                code = code+"\nplt.show()\nprint(result)"
                path = self.config["save_charts_path"]+str(self.last_prompt_id)
                code = code+f"\nplt.savefig({path})\nprint(result)"
                
        try:
            result = super().execute_code(code, output_type)
            assert "Unfortunately" not in result, "\nError at regular execution.Trying alternative..."
            self.new_memory_entry(service="trad_exe_code", prompt=None, code=self.last_code_executed, result=result)
            print("Code sucessfully ran.")
            return result 
        except:
            dfs = [df.pandas_df for df in self.context.dfs]
            if "plt.savefig" in code:
                code = code+"\nplt.show()\nprint(result)"
            else:
                code = code+"\nprint(result)"
            try:
                local_vars = {"dfs":dfs}
                exec(code, globals(), local_vars)
                result = local_vars["result"]
                self.new_memory_entry(service="alt_exe_code", prompt=None, code=code, result=result)
                print("Code sucessfully ran.")
                return result
            except Exception as e:
                raise ValueError(f"Error running stored code: \n{e}")
        
    def chat(self, query: str, output_type=None):
        result = super().chat(query, output_type)
        self.new_memory_entry(service="chat", prompt=query, code=self.last_code_executed, result=result)
        return result
    
    def new_memory_entry(self, service=None, prompt=None, code=None, result=None):
        self.memory[f"chat_{self.counter}"] = {"service": service, "prompt":prompt, "code":code, "result":result}
        self.counter += 1
            
    def code2file(self, chat_number:int=None, filename=".py"):
        """
        Writes the given string of code to a .py file.
        
        :param code_str: String containing the code to be written to the file.
        :param filename: Name of the file to write the code to. Defaults to 'code_file.py'.
        """
        assert chat_number, "Enter a chat_number in order to save it."
        assert chat_number<=self.counter, "Enter a valid chat_number." 
        # Ensure the filename ends with .py
        if not filename.endswith(".py"):
            filename += ".py"
        if filename == ".py":
            filename = f"code_from_chat_{chat_number}.py"
        
        with open(f"data/output/{filename}", "w") as file:
            file.write(self.last_code_generated)
            file.close()
        absolute_file_path = os.path.abspath(f"data/output/{filename}")
        print(f"\nCode written to {absolute_file_path}")

    def save_chat(self, filename="chat.json"):
        if not filename.endswith(".json"):
            filename += ".json"
        self.serialize_dataframe()
        with open(filename, 'w') as file:
            json.dump(self.memory, file, indent=4)
            self.deserialize_dataframe()
        absolute_file_path = os.path.abspath(f"data/output/{filename}")
        print(f"\nCode written to {absolute_file_path}\n")

    def load_chat(self, filename):
        with open(filename, 'r') as file:
            self.memory = json.load(file)
            self.deserialize_dataframe()
            return self
    
    def deserialize_dataframe(self):
        for chat_id, chat_content in self.memory.items():
                if isinstance(chat_content.get('answer'), dict):
                    self.memory[chat_id]["answer"] = pd.DataFrame(self.memory[chat_id]["answer"])

    def serialize_dataframe(self):
        for chat_id, chat_content in self.memory.items():
            if isinstance(chat_content.get('answer'), pd.DataFrame):
                self.memory[chat_id]['answer'] = chat_content.get('answer').to_dict()

    def prepare_data(self):
        data = None
        if isinstance(self.dfs, str):
            if ".db" in self.dfs:
                data = self.prepare_db(self.dfs)
            else:
                raise ValueError("Path must direct to a .db file")
        elif isinstance(self.dfs, list):
            data = self.prepare_df(self.dfs)
        else:
            raise ValueError("Data not valid. It must be a .db file or a list of Dataframe/DataInfo objects")
        return data
    def prepare_df(self, field_descriptions=None):
        data = []
        for df in self.dfs:
            data.append(PandasConnector({"original_df":df.data}, field_descriptions=field_descriptions))
        return data

    def prepare_db(self, db_path="data\\output\\nyctibius.db", field_descriptions=None): 
        data = []
        for table in Modifier(db_path).get_tables():
            data.append(SqliteConnector(config={
            "host": "localhost",
            "port": 3306,
            "username": "root",
            "password": "root",
            "database" : db_path,
            "table": table
        }, field_descriptions=field_descriptions))
        return data
    
    def prepare_config(self, filename="pandasai.json"):
        config = None
        with open(filename, 'r') as file:
            config = json.load(file)
        with open("keys.json", "r") as file:
            keys = json.load(file)
        llm = OpenAI(api_token="sk-"+keys["pandasai2"], temperature=0, seed=888)
        os.environ["PANDASAI_API_KEY"]  = keys["pandasai1"]
        config["llm"]= llm
        return config
    
    def chat_interface(self):
        tarea = False
        prompt = ""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=pd.errors.PerformanceWarning)
            while not tarea:

                o0 = "Improve your prompt before using it."
                o1 = "Chat."
                o2 = "About last interaction."
                o3 = "Not getting what expected? Help the model to understand what you need."
                o4 = "Close chat."

                print(f"\n\nOptions:\n0. {o0} \n1. {o1}\n2. {o2}\n3. {o3}\n4. {o4}")
                option = input("Select an option: ")

                if option == "0":
                    prompt = input("Which prompt do you want to improve?: ")
                    suggested_prompt = self.rephrase_query(prompt)
                    if "y" in input(f"\nThis is a suggested improvement of your prompt:\n\n{suggested_prompt}\n\nDo you want to use it? [Y/N]: ").lower():
                        response = self.chat(suggested_prompt)
                        print(f"\n\nAnswer: {response}")       
                elif option == "1":
                    prompt = input("Chat with your DB: ")
                    response = self.chat(prompt)
                    print(f"\n\nAnswer: {response}")
                elif option == "2":
                    subtarea = False
                    o21 = "Explanation."
                    o22 = "Show code."
                    o23 = "Rerun code."
                    o24 = "Save code to a .py file"
                    o25 = "Back to Menu"
                    while not subtarea:
                        print(f"\n\nOptions:\n1. {o21} \n2. {o22}\n3. {o23}\n4. {o24}\n5. {o25}")
                        suboption = input("Select an option: ")
                        if suboption == "1":
                            response = self.explain()
                            print(f"\n\nExplanation: {response}")
                        elif suboption == "2":
                            print("\n\nCode:\n"+self.last_code_executed)
                        elif suboption == "3":
                            self.execute_code()
                        elif suboption == "4":
                            self.code2file(chat_number=self.counter)
                        elif suboption == "5" or suboption == "q":
                            subtarea = True
                        else:
                            print("Select a proper option")
                elif option == "3":
                    prompt = input("Write the prompt and the model will show 3 questions in order to understand your prompt: ")
                    response = self.clarification_questions(prompt)
                    print(f"\n\Clarification: {response}")
                elif option == "4" or option == "q":
                    if self.memory and "y" == input("Before closing. Do you want to save the chat? [Y/N]: ").lower():
                        self.save_chat(filename=input("Give a name to the json file: "))
                    print("Chat closed")
                    tarea = True
                else:
                    print("Select a proper option")
            return self.memory
